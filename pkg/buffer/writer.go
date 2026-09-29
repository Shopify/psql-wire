package buffer

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Writer provides a convenient way to write pgwire protocol messages
type Writer struct {
	io.Writer
	logger         *slog.Logger
	frame          bytes.Buffer
	batch          bytes.Buffer
	batching       bool
	batchLimit     int
	putbuf         [64]byte // buffer used to construct messages which could be written to the writer frame buffer
	err            error
	ErrorSanitizer func(error) error

	// mu guards the batch state and the fields below because the flush timer
	// runs on its own goroutine. frame is only used by the connection goroutine.
	mu             sync.Mutex
	flushThreshold int
	flushDelay     time.Duration
	flushTimer     *time.Timer
	timerArmed     bool
	flushErr       error
}

// NewWriter constructs a new Postgres buffered message writer for the given io.Writer
func NewWriter(logger *slog.Logger, writer io.Writer) *Writer {
	return &Writer{
		logger: logger,
		Writer: writer,
	}
}

// Start resets the buffer writer and starts a new message with the given
// message type. The message type (byte) and reserved message length bytes (int32)
// are written to the underlaying bytes buffer.
func (writer *Writer) Start(t types.ServerMessage) {
	writer.Reset()
	writer.putbuf[0] = byte(t)
	writer.frame.Write(writer.putbuf[:5]) // message type + message length
}

// AddByte writes the given byte to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *Writer) AddByte(b byte) {
	if writer.err != nil {
		return
	}

	writer.err = writer.frame.WriteByte(b)
}

// AddInt16 writes the given unsigned int16 to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *Writer) AddInt16(i int16) (size int) {
	if writer.err != nil {
		return size
	}

	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(i))
	size, writer.err = writer.frame.Write(x)
	return size
}

// AddInt32 writes the given unsigned int32 to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *Writer) AddInt32(i int32) (size int) {
	if writer.err != nil {
		return size
	}

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(i))
	size, writer.err = writer.frame.Write(x)
	return size
}

// AddBytes writes the given bytes to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *Writer) AddBytes(b []byte) (size int) {
	if writer.err != nil {
		return size
	}

	size, writer.err = writer.frame.Write(b)
	return size
}

// AddString writes the given string to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *Writer) AddString(s string) (size int) {
	if writer.err != nil {
		return size
	}

	size, writer.err = writer.frame.WriteString(s)
	return size
}

// AddNullTerminate writes a null terminate symbol to the end of the given data frame
func (writer *Writer) AddNullTerminate() {
	if writer.err != nil {
		return
	}

	writer.err = writer.frame.WriteByte(0)
}

func (writer *Writer) Error() error {
	return writer.err
}

// Bytes returns the written bytes to the active data frame
func (writer *Writer) Bytes() []byte {
	return writer.frame.Bytes()
}

// Reset resets the data frame to be empty
func (writer *Writer) Reset() {
	writer.frame.Reset()
	writer.err = nil
}

// StartBatch buffers complete frames until threshold bytes have accumulated.
// A non-positive threshold selects 32 KiB. Frames are never split, so a chunk
// can exceed the threshold by the size of its last frame. Batches cannot nest.
// Callers must defer EndBatch immediately, including on error paths, so later
// protocol messages cannot be left in an unflushed batch.
func (writer *Writer) StartBatch(threshold int) {
	if threshold <= 0 {
		threshold = 32 << 10
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.batchLimit = threshold
	writer.batching = true
}

// Buffered returns the number of complete frame bytes awaiting a flush.
func (writer *Writer) Buffered() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.batch.Len()
}

// EndBatch flushes pending complete frames and disables batching, even if the
// flush fails. An incomplete active frame is never included in the batch.
func (writer *Writer) EndBatch() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.batching = false
	return writer.flushLocked()
}

// SetFlushThreshold enables coalescing: outside a batch, frames are buffered
// until n bytes are pending or Flush is called, so callers must Flush before
// waiting on the peer. With n <= 0, the default, frames are written directly.
func (writer *Writer) SetFlushThreshold(n int) {
	writer.flushThreshold = n
}

// SetFlushDelay limits how long coalesced output waits for more frames. With
// d <= 0 it waits for the threshold or an explicit Flush.
func (writer *Writer) SetFlushDelay(d time.Duration) {
	writer.flushDelay = d
}

// Flush writes buffered frames. With coalescing enabled, a write error is also
// returned by every later End and Flush.
func (writer *Writer) Flush() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.flushLocked()
}

func (writer *Writer) flushLocked() error {
	if writer.timerArmed {
		writer.flushTimer.Stop()
		writer.timerArmed = false
	}
	if writer.flushErr != nil {
		return writer.flushErr
	}
	if writer.batch.Len() == 0 {
		return nil
	}
	defer func() {
		// Do not retain an oversized row in both the frame and batch buffers.
		if writer.batch.Cap() > 2*max(writer.batchLimit, writer.flushThreshold) {
			writer.batch = bytes.Buffer{}
		} else {
			writer.batch.Reset()
		}
	}()
	n, err := writer.Writer.Write(writer.batch.Bytes())
	if err == nil && n != writer.batch.Len() {
		err = io.ErrShortWrite
	}
	if err != nil && writer.flushThreshold > 0 {
		writer.flushErr = err
	}
	return err
}

// Write writes p, which must contain whole messages, after any buffered
// output, so that frames encoded elsewhere stay in order.
func (writer *Writer) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	limit := writer.limitLocked()
	if limit <= 0 {
		return writer.Writer.Write(p)
	}
	if err := writer.bufferLocked(p, limit); err != nil {
		return 0, err
	}
	return len(p), nil
}

// limitLocked returns the size at which buffered frames are written, or zero
// when frames are written directly.
func (writer *Writer) limitLocked() int {
	if writer.batching {
		return writer.batchLimit
	}
	return writer.flushThreshold
}

// bufferLocked appends frame and writes the buffer once it holds at least limit
// bytes. Frames are never split, so a write can exceed limit by its last frame.
func (writer *Writer) bufferLocked(frame []byte, limit int) error {
	if writer.flushErr != nil {
		return writer.flushErr
	}
	_, _ = writer.batch.Write(frame)
	if writer.batch.Len() >= limit {
		return writer.flushLocked()
	}
	writer.armTimerLocked()
	return nil
}

// armTimerLocked schedules a flush of the oldest buffered frame. A callback
// that already fired may still be waiting for mu; it then flushes nothing, or
// newer output early, and both are safe.
func (writer *Writer) armTimerLocked() {
	if writer.flushDelay <= 0 || writer.timerArmed {
		return
	}
	writer.timerArmed = true
	if writer.flushTimer == nil {
		writer.flushTimer = time.AfterFunc(writer.flushDelay, writer.delayedFlush)
		return
	}
	writer.flushTimer.Reset(writer.flushDelay)
}

func (writer *Writer) delayedFlush() {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.timerArmed = false
	_ = writer.flushLocked() // sticky; the connection goroutine sees it on its next write
}

// End writes the prepared message to the given writer and resets the buffer.
// The to be expected message length is appended after the message status byte.
func (writer *Writer) End() error {
	defer writer.Reset()
	if writer.Error() != nil {
		return writer.Error()
	}

	bytes := writer.frame.Bytes()
	length := uint32(writer.frame.Len() - 1) // total message length minus the message type byte
	binary.BigEndian.PutUint32(bytes[1:5], length)
	var err error
	writer.mu.Lock()
	if limit := writer.limitLocked(); limit > 0 {
		err = writer.bufferLocked(bytes, limit)
	} else {
		_, err = writer.Writer.Write(bytes)
	}
	writer.mu.Unlock()

	writer.logger.Debug("-> writing message", slog.String("type", types.ServerMessage(bytes[0]).String()))
	return err
}

// EncodeBoolean returns a string value ("on"/"off") representing the given boolean value
func EncodeBoolean(value bool) string {
	if value {
		return "on"
	}

	return "off"
}
