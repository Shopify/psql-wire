package buffer

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

type batchOutput struct {
	packets [][]byte
	err     error
	short   bool
}

func (out *batchOutput) Write(p []byte) (int, error) {
	out.packets = append(out.packets, bytes.Clone(p))
	if out.err != nil {
		return 0, out.err
	}
	if out.short {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func TestBatchFrames(t *testing.T) {
	out := &batchOutput{}
	writer := NewWriter(slogt.New(t), out)
	write := func() {
		writer.Start(types.ServerDataRow)
		writer.AddInt16(0)
		require.NoError(t, writer.End())
	}
	func() {
		writer.StartBatch(14)
		defer func() { require.NoError(t, writer.EndBatch()) }()
		write()
		require.Empty(t, out.packets)
		require.Equal(t, 7, writer.Buffered())
		write()
		require.Len(t, out.packets, 1)
		require.Len(t, out.packets[0], 14)
		require.Zero(t, writer.Buffered())
		write()
	}()
	require.Len(t, out.packets, 2)
	require.Len(t, out.packets[1], 7)
	write()
	require.Len(t, out.packets, 3, "later messages are unbuffered")
	require.NoError(t, writer.EndBatch())
	require.Len(t, out.packets, 3, "empty EndBatch does not write")
}

func TestBatchFrameErrorPreservesEarlierFrames(t *testing.T) {
	out := &batchOutput{}
	writer := NewWriter(slogt.New(t), out)
	frameErr := errors.New("bad frame")
	func() {
		writer.StartBatch(0)
		defer func() { require.NoError(t, writer.EndBatch()) }()
		writer.Start(types.ServerDataRow)
		writer.AddInt16(0)
		require.NoError(t, writer.End())
		writer.Start(types.ServerDataRow)
		writer.err = frameErr
		require.ErrorIs(t, writer.End(), frameErr)
		require.Empty(t, writer.Bytes())
		require.NoError(t, writer.Error())
	}()
	require.Len(t, out.packets, 1)
	require.Equal(t, []byte{'D', 0, 0, 0, 6, 0, 0}, out.packets[0])
}

func TestBatchFlushErrors(t *testing.T) {
	outputErr := errors.New("output failed")
	for _, threshold := range []int{1, 32 << 10} {
		for _, short := range []bool{false, true} {
			out := &batchOutput{short: short}
			wantErr := io.ErrShortWrite
			if !short {
				out.err = outputErr
				wantErr = outputErr
			}
			writer := NewWriter(slogt.New(t), out)
			func() {
				writer.StartBatch(threshold)
				defer func() {
					err := writer.EndBatch()
					if threshold == 1 {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, wantErr)
					}
				}()
				writer.Start(types.ServerDataRow)
				writer.AddInt16(0)
				err := writer.End()
				if threshold == 1 {
					require.ErrorIs(t, err, wantErr)
				} else {
					require.NoError(t, err)
				}
			}()
			require.Len(t, out.packets, 1, "a failed write is not retried")
			require.False(t, writer.batching)
			require.Zero(t, writer.Buffered())
		}
	}
}

func TestBatchOversizedFrameReleasesStorage(t *testing.T) {
	out := &batchOutput{}
	writer := NewWriter(slogt.New(t), out)
	func() {
		writer.StartBatch(0)
		defer func() { require.NoError(t, writer.EndBatch()) }()
		writer.Start(types.ServerDataRow)
		writer.AddBytes(make([]byte, 128<<10))
		require.NoError(t, writer.End())
		require.Len(t, out.packets, 1)
		require.Len(t, out.packets[0], (128<<10)+5)
		require.Zero(t, writer.batch.Cap())
	}()
}
