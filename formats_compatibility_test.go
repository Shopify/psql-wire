package wire

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatsMatchNegotiatedWireOutput(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, formats := range [][]int16{nil, {1}, {0, 1}} {
			t.Run(fmt.Sprintf("parallel=%t/formats=%v", parallel, formats), func(t *testing.T) {
				captured := make(chan []FormatCode, 1)
				conn := compatibilityClient(t, func(context.Context, string) (PreparedStatements, error) {
					return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
						captured <- append([]FormatCode(nil), w.Formats()...)
						if err := w.Row([]any{"x", int32(7)}); err != nil {
							return err
						}
						return w.Complete("SELECT 1")
					}, WithColumns(Columns{{Name: "text", Oid: 25}, {Name: "int", Oid: 23}}))), nil
				}, ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
				result := conn.ExecParams(context.Background(), "formats", nil, nil, nil, formats).Read()
				require.NoError(t, result.Err)
				require.Len(t, result.Rows, 1)
				require.Len(t, result.FieldDescriptions, 2)
				var want []FormatCode
				for _, format := range formats {
					want = append(want, FormatCode(format))
				}
				require.Equal(t, want, <-captured)
				for i, field := range result.FieldDescriptions {
					expected := int16(0)
					if len(formats) == 1 {
						expected = formats[0]
					} else if len(formats) > 1 {
						expected = formats[i]
					}
					require.Equal(t, expected, field.Format)
				}
				require.Equal(t, []byte("x"), result.Rows[0][0])
				if len(formats) == 0 {
					require.Equal(t, []byte("7"), result.Rows[0][1])
				} else {
					require.Equal(t, []byte{0, 0, 0, 7}, result.Rows[0][1])
				}
			})
		}
	}
}
