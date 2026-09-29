package wire

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

type handshakeResult struct {
	version types.Version
	reader  *buffer.Reader
	err     error
}

func startupPacket(version types.Version, parameters ...string) []byte {
	size := 8
	for _, parameter := range parameters {
		size += len(parameter) + 1
	}
	if len(parameters) > 0 {
		size++
	}
	packet := make([]byte, size)
	binary.BigEndian.PutUint32(packet[0:4], uint32(size))
	binary.BigEndian.PutUint32(packet[4:8], uint32(version))
	offset := 8
	for _, parameter := range parameters {
		copy(packet[offset:], parameter)
		offset += len(parameter) + 1
	}
	return packet
}

func runHandshake(t *testing.T, packets ...[]byte) (net.Conn, <-chan handshakeResult) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(5*time.Second)))
	server, err := NewServer(nil, MessageBufferSize(4096))
	require.NoError(t, err)

	result := make(chan handshakeResult, 1)
	go func() {
		_, version, reader, err := server.Handshake(serverConn)
		result <- handshakeResult{version: version, reader: reader, err: err}
	}()

	payload := make([]byte, 0)
	for _, packet := range packets {
		payload = append(payload, packet...)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write(payload)
		writeDone <- err
	}()
	t.Cleanup(func() {
		select {
		case <-writeDone:
		default:
		}
	})
	return clientConn, result
}

func awaitHandshake(t *testing.T, result <-chan handshakeResult) handshakeResult {
	t.Helper()
	select {
	case handshake := <-result:
		return handshake
	case <-time.After(time.Second):
		t.Fatal("handshake did not complete")
		return handshakeResult{}
	}
}

func TestGSSNegotiationPreservesPipelinedStartup(t *testing.T) {
	client, result := runHandshake(t,
		startupPacket(types.VersionGSSENC),
		startupPacket(types.Version30, "user", "mock"),
	)

	response := []byte{0}
	_, err := io.ReadFull(client, response)
	require.NoError(t, err)
	require.Equal(t, []byte{'N'}, response)

	handshake := awaitHandshake(t, result)
	require.NoError(t, handshake.err)
	require.Equal(t, types.Version30, handshake.version)
	user, err := handshake.reader.GetString()
	require.NoError(t, err)
	require.Equal(t, "user", user)
	value, err := handshake.reader.GetString()
	require.NoError(t, err)
	require.Equal(t, "mock", value)
}

func TestGSSNegotiationCanContinueToSSLRequest(t *testing.T) {
	client, result := runHandshake(t,
		startupPacket(types.VersionGSSENC),
		startupPacket(types.VersionSSLRequest),
		startupPacket(types.Version30, "user", "mock"),
	)

	response := make([]byte, 2)
	_, err := io.ReadFull(client, response)
	require.NoError(t, err)
	require.Equal(t, []byte{'N', 'N'}, response)

	handshake := awaitHandshake(t, result)
	require.NoError(t, handshake.err)
	require.Equal(t, types.Version30, handshake.version)
}

func TestRepeatedGSSNegotiationIsRejected(t *testing.T) {
	client, result := runHandshake(t,
		startupPacket(types.VersionGSSENC),
		startupPacket(types.VersionGSSENC),
	)

	response := []byte{0}
	_, err := io.ReadFull(client, response)
	require.NoError(t, err)
	require.Equal(t, []byte{'N'}, response)

	handshake := awaitHandshake(t, result)
	require.ErrorContains(t, handshake.err, "repeated GSS encryption request")
	require.Equal(t, types.VersionGSSENC, handshake.version)
}
