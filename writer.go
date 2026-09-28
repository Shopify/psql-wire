package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]any) error

	// Written returns the number of rows written to the client.
	Written() uint32

	// Empty announces to the client an empty response and that no data rows should
	// be expected.
	Empty() error

	// Columns returns the columns that are currently defined within the writer.
	Columns() Columns

	// Formats returns the per-column wire format codes negotiated for the
	// current portal. The slice is read-only — callers must not mutate it.
	// An empty slice means no formats were negotiated (the default text
	// format applies to every column).
	Formats() []FormatCode

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	//
	// See [CommandComplete] for the expected format for different queries.
	//
	// [CommandComplete]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
	Complete(description string) error

	// CopyIn sends a [CopyInResponse] to the client, to initiate a CopyIn
	// operation. The copy operation can be used to send large amounts of data to
	// the server in a single transaction. A column reader has to be used to read
	// the data that is sent by the client to the CopyReader.
	CopyIn(format FormatCode) (*CopyReader, error)
}

// RowWriter is an optional DataWriter extension. Rows must be observably
// identical to calling Row once per row, in order.
type RowWriter interface {
	Rows(rows [][]any) error
}

// WriteRows writes rows in order, using batching when supported by w.
func WriteRows(w DataWriter, rows [][]any) error {
	if bw, ok := w.(RowWriter); ok {
		return bw.Rows(rows)
	}
	for _, row := range rows {
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return nil
}

// ErrDataWritten is returned when an empty result is attempted to be sent to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is returned when the data writer has been closed.
var ErrClosedWriter = errors.New("closed writer")

// ErrRowLimitExceeded is returned only when portal suspension is disabled.
var ErrRowLimitExceeded = pgerror.WithCode(errors.New("row limit exceeded"), codes.ProgramLimitExceeded)

// Bound per-writer scratch so a single large value is not retained.
const maxEncodeScratchCapacity = 64 << 10

// dataWriter implements DataWriter for use inside an iter.Seq push
// iterator. Row encodes the row to the wire and then yields to the pull
// consumer for flow control. Complete writes CommandComplete to the wire.
// This approach allows portal suspension: when the pull consumer stops
// pulling (row limit reached), the handler goroutine blocks in yield
// until the next Execute.
type dataWriter struct {
	ctx     context.Context
	session *Session
	columns Columns
	formats []FormatCode
	client  *buffer.Writer
	reader  *buffer.Reader
	yield   func(struct{}) bool
	tag     *string
	closed  bool
	written uint32
	limit   Limit // legacy fail-at-limit mode; zero means no limit
	// Only the first unlimited Execute can enable batching. Limited portals
	// must continue yielding per row, including after subsequent Executes.
	batchable     bool
	encodeScratch []byte

	// encodeObserver is captured from ctx when the handler starts. While it
	// is set, Row accumulates per-column totals in encodeStats instead of
	// calling the observer per value. Portal.execute publishes them at every
	// Execute boundary: suspension, completion, handler error and teardown.
	encodeObserver EncodeObserver
	encodeStats    []encodeStats
}

// observeEncoding enables aggregated encode observation when the context
// carries an EncodeObserver.
func (writer *dataWriter) observeEncoding() {
	writer.encodeObserver = EncodeObserverFromContext(writer.ctx)
	if writer.encodeObserver != nil {
		writer.encodeStats = make([]encodeStats, len(writer.columns))
	}
}

func (writer *dataWriter) Columns() Columns {
	return writer.columns
}

func (writer *dataWriter) Formats() []FormatCode {
	return writer.formats
}

func (writer *dataWriter) Row(values []any) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.limit != NoLimit && Limit(writer.written) >= writer.limit {
		return ErrRowLimitExceeded
	}

	// No per-value observer: encodeStats is published at Execute boundaries.
	err := writer.columns.write(writer.ctx, writer.formats, writer.client, values, TypeMap(writer.ctx), &writer.encodeScratch, nil, writer.encodeStats)
	if err != nil {
		return err
	}

	writer.written++
	// The yield call "teleports" us back the next call of the pull consumer in
	// Portal.execute. The yield function returns true when the pull consumer
	// calls next again, and returns false when stop is called.
	if !writer.yield(struct{}{}) {
		return ErrSuspendedHandlerClosed
	}
	return nil
}

// Rows writes materialized rows, batching frames only when portal flow control
// permits it. Limited portals keep Row's per-row suspension checkpoints, and a
// legacy PortalSuspension(false) limit keeps Row's ErrRowLimitExceeded check.
func (writer *dataWriter) Rows(rows [][]any) (err error) {
	if writer.closed {
		return ErrClosedWriter
	}
	if !writer.batchable || writer.limit != NoLimit {
		for _, row := range rows {
			if err := writer.Row(row); err != nil {
				return err
			}
		}
		return nil
	}

	writer.client.StartBatch(32 << 10)
	defer func() {
		pending := writer.client.Buffered() > 0
		flushErr := writer.client.EndBatch()
		if err == nil {
			err = flushErr
		}
		if pending && flushErr == nil && !writer.yield(struct{}{}) && err == nil {
			err = ErrSuspendedHandlerClosed
		}
	}()

	tm := TypeMap(writer.ctx)
	for _, row := range rows {
		if err := writer.columns.write(writer.ctx, writer.formats, writer.client, row, tm, &writer.encodeScratch, nil, writer.encodeStats); err != nil {
			return err
		}
		writer.written++
		if writer.client.Buffered() == 0 && !writer.yield(struct{}{}) {
			return ErrSuspendedHandlerClosed
		}
	}
	return nil
}

func (writer *dataWriter) CopyIn(format FormatCode) (*CopyReader, error) {
	if writer.closed {
		return nil, ErrClosedWriter
	}

	err := writer.columns.CopyIn(writer.ctx, writer.client, format)
	if err != nil {
		return nil, err
	}
	return NewCopyReader(writer.session, writer.reader, writer.client, writer.columns), nil
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written != 0 {
		return ErrDataWritten
	}

	defer writer.close()
	return nil
}

func (writer *dataWriter) Written() uint32 {
	return writer.written
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	defer writer.close()
	*writer.tag = description
	return commandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
	writer.flushEncodeObservations()
	writer.encodeScratch = nil
}

// flushEncodeObservations publishes the accumulated per-column totals and
// resets them, so each call reports only values encoded since the previous
// one. It must run on the goroutine that owns the handler: either the handler
// itself or Portal.execute while the handler is parked in yield.
func (writer *dataWriter) flushEncodeObservations() {
	if writer.encodeObserver == nil {
		return
	}
	for index := range writer.encodeStats {
		stat := writer.encodeStats[index]
		if stat.count == 0 {
			continue
		}
		writer.encodeStats[index] = encodeStats{}

		format := TextFormat
		if len(writer.formats) > 0 {
			format = writer.formats[0]
			if len(writer.formats) > index {
				format = writer.formats[index]
			}
		}
		writer.encodeObserver(writer.ctx, format, writer.columns[index].Oid, stat.count, stat.encodedBytes)
	}
}

// commandComplete announces that the requested command has successfully been executed.
// The given description is written back to the client and could be used to send
// additional meta data to the user.
func commandComplete(writer *buffer.Writer, description string) error {
	writer.Start(types.ServerCommandComplete)
	writer.AddString(description)
	writer.AddNullTerminate()
	return writer.End()
}

// ErrSuspendedHandlerClosed is returned from DataWriter.Row when a suspended
// portal is closed (or re-bound) before the handler finished producing rows.
// Handlers can check for this error to distinguish graceful portal teardown
// from real failures and skip error logging.
var ErrSuspendedHandlerClosed = errors.New("suspended handler closed")
