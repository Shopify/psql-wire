package buffer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

// recordingWriter records each Write separately.
type recordingWriter struct {
	mu     sync.Mutex
	writes [][]byte
	err    error
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	w.writes = append(w.writes, bytes.Clone(p))
	return len(p), nil
}

func (w *recordingWriter) setErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = err
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

// sequences returns the sequence numbers of the messages in each write and
// fails if a write splits a message.
func (w *recordingWriter) sequences(t *testing.T) [][]uint32 {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	var out [][]uint32
	for _, write := range w.writes {
		var seqs []uint32
		for len(write) > 0 {
			require.GreaterOrEqual(t, len(write), 5, "write ends inside a message header")
			size := int(binary.BigEndian.Uint32(write[1:5]))
			require.GreaterOrEqual(t, len(write), 1+size, "write ends inside a message")
			seqs = append(seqs, binary.BigEndian.Uint32(write[5:9]))
			write = write[1+size:]
		}
		out = append(out, seqs)
	}
	return out
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// writeMessage writes a DataRow whose payload starts with seq, padded to size.
func writeMessage(t *testing.T, writer *Writer, seq uint32, size int) {
	t.Helper()
	payload := make([]byte, max(size, 4))
	binary.BigEndian.PutUint32(payload, seq)
	writer.Start(types.ServerDataRow)
	writer.AddBytes(payload)
	require.NoError(t, writer.End())
}

func TestWriterCoalescesWholeMessages(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(64)

	for seq := range uint32(3) {
		writeMessage(t, writer, seq, 10) // 15 bytes each
	}
	require.Zero(t, sink.count())
	require.Equal(t, 45, writer.Buffered())

	writeMessage(t, writer, 3, 20)  // crosses the threshold and is written with the rest
	writeMessage(t, writer, 4, 100) // exceeds the threshold on its own
	require.Equal(t, 2, sink.count())
	require.Zero(t, writer.Buffered())

	require.NoError(t, writer.Flush())
	require.Equal(t, [][]uint32{{0, 1, 2, 3}, {4}}, sink.sequences(t))
}

func TestWriterWritesThroughWithoutThreshold(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	for seq := range uint32(3) {
		writeMessage(t, writer, seq, 10)
	}
	require.Equal(t, [][]uint32{{0}, {1}, {2}}, sink.sequences(t))
	require.NoError(t, writer.Flush())
	require.Zero(t, writer.Buffered())
}

func TestWriterBatchSharesCoalescingBuffer(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(1 << 10)

	writeMessage(t, writer, 0, 10)
	writer.StartBatch(40)
	for seq := uint32(1); seq <= 4; seq++ {
		writeMessage(t, writer, seq, 10)
	}
	require.NoError(t, writer.EndBatch())
	writeMessage(t, writer, 5, 10)
	require.NoError(t, writer.Flush())

	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5}, slices.Concat(sink.sequences(t)...))
}

// TestWriterWriteKeepsOrder writes frames encoded elsewhere, as the pipelined
// response queue does, after a buffered frame.
func TestWriterWriteKeepsOrder(t *testing.T) {
	var encoded bytes.Buffer
	encoder := NewWriter(testLogger(), &encoded)
	writeMessage(t, encoder, 1, 10)
	writeMessage(t, encoder, 2, 10)

	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(1 << 10)
	writeMessage(t, writer, 0, 10)
	n, err := writer.Write(encoded.Bytes())
	require.NoError(t, err)
	require.Equal(t, encoded.Len(), n)
	require.Zero(t, sink.count())

	require.NoError(t, writer.Flush())
	require.Equal(t, [][]uint32{{0, 1, 2}}, sink.sequences(t))
}

func TestWriterFlushErrorIsSticky(t *testing.T) {
	failure := errors.New("connection reset")
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(1 << 10)

	writeMessage(t, writer, 0, 10)
	sink.setErr(failure)
	require.ErrorIs(t, writer.Flush(), failure)

	// The stream is inconsistent even if the connection would accept bytes again.
	sink.setErr(nil)
	writer.Start(types.ServerDataRow)
	writer.AddBytes([]byte("next"))
	require.ErrorIs(t, writer.End(), failure)
	require.ErrorIs(t, writer.Flush(), failure)
	require.Zero(t, sink.count())
}

func TestWriterFlushesAfterDelay(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(1 << 10)
	writer.SetFlushDelay(time.Millisecond)

	writeMessage(t, writer, 0, 10)
	require.Eventually(t, func() bool { return sink.count() == 1 }, 5*time.Second, time.Millisecond)
	writeMessage(t, writer, 1, 10)
	require.Eventually(t, func() bool { return sink.count() == 2 }, 5*time.Second, time.Millisecond)
	require.Zero(t, writer.Buffered())
}

func TestWriterWithoutDelayWaitsForFlush(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(1 << 10)

	writeMessage(t, writer, 0, 10)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, sink.count())
	require.NoError(t, writer.Flush())
	require.Equal(t, 1, sink.count())
}

// TestWriterDelayedFlushKeepsOrder races the flush timer against the writer;
// run it with -race.
func TestWriterDelayedFlushKeepsOrder(t *testing.T) {
	sink := &recordingWriter{}
	writer := NewWriter(testLogger(), sink)
	writer.SetFlushThreshold(256)
	writer.SetFlushDelay(time.Microsecond)

	const n = 2000
	want := make([]uint32, n)
	for seq := range uint32(n) {
		writeMessage(t, writer, seq, 7)
		want[seq] = seq
	}
	require.NoError(t, writer.Flush())
	require.Equal(t, want, slices.Concat(sink.sequences(t)...))
}

func TestReaderBeforeReadRunsOnlyWhenRefilling(t *testing.T) {
	message := []byte{'Q', 0, 0, 0, 5, 'x'}
	reader := NewReader(testLogger(), bytes.NewReader(slices.Concat(message, message)), DefaultBufferSize)
	calls := 0
	reader.BeforeRead(func() error {
		calls++
		return nil
	})

	for range 2 {
		_, _, err := reader.ReadTypedMsg()
		require.NoError(t, err)
	}
	require.Equal(t, 1, calls, "both messages come from one read")
	_, _, err := reader.ReadTypedMsg()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, calls)

	failure := errors.New("flush failed")
	failing := NewReader(testLogger(), bytes.NewReader(message), DefaultBufferSize)
	failing.BeforeRead(func() error { return failure })
	_, _, err = failing.ReadTypedMsg()
	require.ErrorIs(t, err, failure)
}
