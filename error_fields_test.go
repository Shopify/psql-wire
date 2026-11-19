package wire

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestErrorResponseFields(t *testing.T) {
	for _, severity := range []psqlerr.Severity{psqlerr.LevelError, psqlerr.LevelFatal, psqlerr.LevelPanic} {
		for _, line := range []int32{0, 123, math.MaxInt32} {
			t.Run(fmt.Sprintf("%s/line_%d", severity, line), func(t *testing.T) {
				err := psqlerr.WithCode(errors.New("duplicate key"), codes.UniqueViolation)
				err = psqlerr.WithSeverity(err, severity)
				err = psqlerr.WithDetail(err, "Key already exists.")
				err = psqlerr.WithHint(err, "Choose a different key.")
				err = psqlerr.WithConstraintName(err, "items_pkey")
				err = psqlerr.WithSource(err, "constraints.go", line, "checkUnique")

				var sink bytes.Buffer
				writer := buffer.NewWriter(slogt.New(t), &sink)
				require.NoError(t, WriteUnterminatedError(writer, err))

				// Decode the complete frame as a client would, including its length.
				client := pgproto3.NewFrontend(&sink, io.Discard)
				message, decodeErr := client.Receive()
				require.NoError(t, decodeErr)
				require.Equal(t, &pgproto3.ErrorResponse{
					Severity:            string(severity),
					SeverityUnlocalized: string(severity),
					Code:                "23505",
					Message:             "duplicate key",
					Detail:              "Key already exists.",
					Hint:                "Choose a different key.",
					ConstraintName:      "items_pkey",
					File:                "constraints.go",
					Line:                line,
					Routine:             "checkUnique",
				}, message)
			})
		}
	}
}

func TestErrorResponseOptionalFields(t *testing.T) {
	for _, sanitize := range []bool{false, true} {
		t.Run(fmt.Sprintf("sanitize_%t", sanitize), func(t *testing.T) {
			publicErr := psqlerr.WithCode(errors.New("request failed"), codes.Internal)
			inputErr := publicErr
			var sink bytes.Buffer
			writer := buffer.NewWriter(slogt.New(t), &sink)
			if sanitize {
				inputErr = psqlerr.WithSource(inputErr, "private.go", 123, "privateRoutine")
				inputErr = psqlerr.WithConstraintName(inputErr, "private_constraint")
				writer.ErrorSanitizer = func(error) error { return publicErr }
			}
			require.NoError(t, WriteUnterminatedError(writer, inputErr))
			message, err := pgproto3.NewFrontend(&sink, io.Discard).Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity:            "ERROR",
				SeverityUnlocalized: "ERROR",
				Code:                "XX000",
				Message:             "request failed",
			}, message)
		})
	}
}
