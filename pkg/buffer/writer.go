package buffer

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Writer provides a convenient way to write pgwire protocol messages
type Writer struct {
	io.Writer
	logger         *slog.Logger
	frame          bytes.Buffer
	batch          bytes.Buffer
	batching       bool
	session        bool // session-scoped buffering: EndBatch keeps buffering; Flush sends
	batchLimit     int
	putbuf         [64]byte // buffer used to construct messages which could be written to the writer frame buffer
	err            error
	ErrorSanitizer func(error) error
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
	if writer.session {
		// Session buffering already coalesces every frame until Flush; a
		// nested row batch has nothing to add and must not change the limit.
		return
	}
	if threshold <= 0 {
		threshold = 32 << 10
	}
	writer.batchLimit = threshold
	writer.batching = true
}

// StartSessionBuffering buffers complete frames until Flush is called or
// threshold bytes accumulate, like the PostgreSQL backend's send buffer. A
// non-positive threshold selects 8 KiB. It stays on for the writer's lifetime;
// StartBatch and EndBatch become no-ops, so callers must Flush at protocol
// points where the client waits for output.
func (writer *Writer) StartSessionBuffering(threshold int) {
	if threshold <= 0 {
		threshold = 8 << 10
	}
	writer.batchLimit = threshold
	writer.batching = true
	writer.session = true
}

// Flush sends every buffered frame to the underlying writer and keeps the
// current buffering mode. It is a no-op when nothing is buffered.
func (writer *Writer) Flush() error {
	return writer.flushBatch()
}

// WriteFrames appends already-framed protocol bytes (for example a pre-rendered
// Execute result) to the current batch, or writes them through when batching is
// off. A payload at or above the batch limit is written directly after the
// pending batch is flushed, so large results are not copied a second time.
func (writer *Writer) WriteFrames(frames []byte) (int, error) {
	if !writer.batching {
		return writer.Write(frames)
	}
	if len(frames) >= writer.batchLimit {
		if err := writer.flushBatch(); err != nil {
			return 0, err
		}
		return writer.Write(frames)
	}
	n, _ := writer.batch.Write(frames)
	if writer.batch.Len() >= writer.batchLimit {
		if err := writer.flushBatch(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Buffered returns the number of complete frame bytes awaiting a batch flush.
func (writer *Writer) Buffered() int {
	return writer.batch.Len()
}

// EndBatch flushes pending complete frames and disables batching, even if the
// flush fails. An incomplete active frame is never included in the batch.
func (writer *Writer) EndBatch() error {
	if writer.session {
		return nil
	}
	writer.batching = false
	return writer.flushBatch()
}

func (writer *Writer) flushBatch() error {
	if writer.batch.Len() == 0 {
		return nil
	}
	defer func() {
		// Do not retain an oversized row in both the frame and batch buffers.
		if writer.batch.Cap() > 2*writer.batchLimit {
			writer.batch = bytes.Buffer{}
		} else {
			writer.batch.Reset()
		}
	}()
	n, err := writer.Write(writer.batch.Bytes())
	if err == nil && n != writer.batch.Len() {
		err = io.ErrShortWrite
	}
	return err
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
	if writer.batching {
		_, _ = writer.batch.Write(bytes)
		if writer.batch.Len() >= writer.batchLimit {
			err = writer.flushBatch()
		}
	} else {
		_, err = writer.Write(bytes)
	}

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
