package wire

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TListenAndServe will open a new TCP listener on a unallocated port inside
// the local network. The newly created listener is passed to the given server to
// start serving PostgreSQL connections. The full listener address is returned
// for clients to interact with the newly created server.
func TListenAndServe(t *testing.T, server *Server) *net.TCPAddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := server.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	go server.Serve(listener) //nolint:errcheck
	return listener.Addr().(*net.TCPAddr)
}

func TestClientConnect(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			return writer.Complete("OK")
		})

		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("mock", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		if err != nil {
			t.Fatal(err)
		}

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t)
		client.Close(t)
	})

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Ping()
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Ping(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientParameters(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			writer.Row([]any{"John Doe"}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "full_name",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query)))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT * FROM users WHERE age > ?", 50)
		if err != nil {
			t.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT * FROM users WHERE age > ?", 50)
		if err != nil {
			t.Fatal(err)
		}

		rows.Close()

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerWritingResult(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{"John", true, 28})   //nolint:errcheck
			writer.Row([]any{"Marry", false, 21}) //nolint:errcheck
			return writer.Complete("SELECT 2")
		}

		columns := Columns{ //nolint:errcheck
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
			{
				Table: 0,
				Name:  "member",
				Oid:   oid.T_bool,
				Width: 1,
			},
			{
				Table: 0,
				Name:  "age",
				Oid:   oid.T_int4,
				Width: 1,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			var name string
			var member bool
			var age int

			err := rows.Scan(&name, &member, &age)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %s, %d, %t", name, age, member)
		}
		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			var name string
			var member bool
			var age int

			err := rows.Scan(&name, &member, &age)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %s, %d, %t", name, age, member)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerHandlingMultipleConnections(t *testing.T) {
	address := TOpenMockServer(t)
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	conn, err := sql.Open("pgx", connstr)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	err = conn.Ping()
	require.NoError(t, err)

	t.Run("simple query", func(t *testing.T) {
		rows, err := conn.Query("select age from person")
		require.NoError(t, err)
		t.Cleanup(func() {
			rows.Close()
		})
		assert.True(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("prepared statements", func(t *testing.T) {
		tests := []string{
			"select age from person where age > $1",
			"select age from person where age > ?",
		}

		for _, query := range tests {
			t.Run(query, func(t *testing.T) {
				stmt, err := conn.Prepare(query)
				require.NoError(t, err)
				t.Cleanup(func() {
					stmt.Close()
				})
				rows, err := stmt.Query(1)
				require.NoError(t, err)
				t.Cleanup(func() {
					rows.Close()
				})
				require.True(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}
	})
}

func TOpenMockServer(t *testing.T) *net.TCPAddr {
	t.Helper()
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{20}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "age",
				Oid:   oid.T_int4,
				Width: 1,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query)))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)
	return address
}

func TestServerNULLValues(t *testing.T) {
	t.Parallel()

	name := "John"
	expected := []*string{
		&name,
		nil,
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{"John"}) //nolint:errcheck
			writer.Row([]any{nil})    //nolint:errcheck
			return writer.Complete("SELECT 2")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		result := []*string{}
		for rows.Next() {
			var name *string
			err := rows.Scan(&name)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %+v", name)
			result = append(result, name)
		}

		if len(result) != len(expected) {
			t.Fatal("an unexpected amount of records was returned")
		}

		for index := range expected {
			switch {
			case expected[index] == nil:
				if result[index] != nil {
					t.Errorf("unexpected value %+v, expected nil", result[index])
				}
			case expected[index] != nil:
				left := *expected[index]
				right := *result[index]

				if left != right {
					t.Errorf("unexpected value %+v, expected %+v", left, right)
				}
			}
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		result := []*string{}
		for rows.Next() {
			var name *string
			err := rows.Scan(&name)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %+v", name)
			result = append(result, name)
		}

		for index := range expected {
			switch {
			case expected[index] == nil:
				if result[index] != nil {
					t.Errorf("unexpected value %+v, expected nil", result[index])
				}
			case expected[index] != nil:
				left := *expected[index]
				right := *result[index]

				if left != right {
					t.Errorf("unexpected value %+v, expected %+v", left, right)
				}
			}
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSessionAttributes(t *testing.T) {
	t.Parallel()

	var sessionAttributes map[string]interface{}
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		SetAttribute(ctx, "test_key", "test_value")
		SetAttribute(ctx, "numeric_key", 42)

		sess, ok := GetSession(ctx)
		if ok {
			sessionAttributes = sess.Attributes
		}

		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})

		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	conn, err := sql.Open("postgres", connstr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Exec("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "test_value", sessionAttributes["test_key"])
	assert.Equal(t, 42, sessionAttributes["numeric_key"])

	ctx := context.Background()
	sess := &Session{Attributes: map[string]interface{}{"foo": "bar"}}
	ctx = context.WithValue(ctx, sessionKey, sess)

	val, ok := GetAttribute(ctx, "foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", val)

	val, ok = GetAttribute(ctx, "non_existent")
	assert.False(t, ok)
	assert.Nil(t, val)

	ok = SetAttribute(ctx, "new_key", "new_value")
	assert.True(t, ok)
	assert.Equal(t, "new_value", sess.Attributes["new_key"])
}

func TestServerCopyIn(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		t.Log("preparing query", query)

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("copying data")

			c, err := writer.CopyIn(BinaryFormat)
			if err != nil {
				return err
			}

			b, err := NewBinaryColumnReader(ctx, c)
			if err != nil {
				return err
			}

			for {
				rows, err := b.Read(ctx)
				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				t.Logf("received columns: %+v", rows)
			}

			return writer.Complete("COPY 2")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "id",
				Oid:   oid.T_int4,
				Width: 1,
			},
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
			{
				Table: 0,
				Name:  "spotify_id",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	rows := [][]any{
		{196, "My Posse In Effect", nil},
		{181, "Almost KISS", "10"},
	}

	t.Run("lib/pq", func(t *testing.T) {
		t.Skip()
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		txn, err := conn.Begin()
		if err != nil {
			t.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("id", "name", "spotify_id"))
		if err != nil {
			t.Fatal(err)
		}

		for _, row := range rows {
			_, err := stmt.Exec(row...)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err != nil {
			t.Fatal(err)
		}

		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		n, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"id", "name", "spotify_id"}, pgx.CopyFromRows(rows))
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("unexpected number of rows copied: %d", n)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}
