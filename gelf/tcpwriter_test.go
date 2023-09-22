package gelf

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewTCPWriter(t *testing.T) {
	w, err := NewTCPWriter("")
	if err == nil && w != nil {
		t.Error("New didn't fail")
		return
	}
}

func TestNewTCPWriterConfig(t *testing.T) {
	r, _, _, err := newTCPReader("127.0.0.1:0")
	require.NoError(t, err)

	w, err := NewTCPWriter(r.addr())
	require.NoError(t, err)

	require.Equalf(t, DefaultMaxReconnect, w.MaxReconnect, "Default MaxReconnect")
	w.MaxReconnect = 5
	require.Equalf(t, 5, w.MaxReconnect, "Custom MaxReconnect")

	require.Equalf(t, time.Duration(DefaultReconnectDelay), w.ReconnectDelay, "Default ReconnectDelay")
	w.ReconnectDelay = 5
	require.Equalf(t, time.Duration(5), w.ReconnectDelay, "Custom ReconnectDelay")
}

func assertMessages(t *testing.T, msg *Message, short, full string) {
	t.Helper()

	require.Equalf(t, short, msg.Short, "msg.Short")
	require.Equalf(t, full, msg.Full, "msg.Full")
}

func assertHasSuffix(t *testing.T, str string, sfx string, msg string) {
	t.Helper()

	if !strings.HasSuffix(str, sfx) {
		t.Errorf("expected %s, got %s: %s", sfx, str, msg)
	}
}

func TestWriteSmallMultiLineTCP(t *testing.T) {
	msgData := "awesomesauce\nbananas"

	msg := sendAndRecvTCP(t, msgData)

	assertMessages(t, msg, "awesomesauce", msgData)
}

func TestWriteSmallOneLineTCP(t *testing.T) {
	msgData := "some awesome thing\n"
	msgDataTrunc := msgData[:len(msgData)-1]

	msg := sendAndRecvTCP(t, msgData)

	assertMessages(t, msg, msgDataTrunc, "")

	assertHasSuffix(t, msg.Extra["_file"].(string), "/go-gelf/gelf/tcpwriter_test.go", "")

	require.Len(t, msg.Extra, 2, "expect only file and line")
}

func TestWriteBigMessageTCP(t *testing.T) {
	randData := make([]byte, 4096)
	_, err := rand.Read(randData)
	require.NoError(t, err)

	msgData := "awesomesauce\n" + base64.StdEncoding.EncodeToString(randData)

	msg := sendAndRecvTCP(t, msgData)

	assertMessages(t, msg, "awesomesauce", msgData)
}

func TestWriteMultiPacketMessageTCP(t *testing.T) {
	randData := make([]byte, 150000)
	_, err := rand.Read(randData)
	require.NoError(t, err)

	msgData := "awesomesauce\n" + base64.StdEncoding.EncodeToString(randData)

	msg := sendAndRecvTCP(t, msgData)

	assertMessages(t, msg, "awesomesauce", msgData)
}

func TestExtraDataTCP(t *testing.T) {

	// time.Now().Unix() seems fine, UnixNano() won't roundtrip
	// through string -> float64 -> int64
	extra := map[string]interface{}{
		"_a":    10 * time.Now().Unix(),
		"C":     9,
		"_file": "writer_test.go",
		"_line": 186,
	}

	short := "quick"
	full := short + "\nwith more detail"
	m := Message{
		Version:  "1.0",
		Host:     "fake-host",
		Short:    string(short),
		Full:     string(full),
		TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
		Level:    6, // info
		Facility: "writer_test",
		Extra:    extra,
		RawExtra: []byte(`{"woo": "hoo"}`),
	}

	msg := sendAndRecvMsgTCP(t, &m)
	assertMessages(t, msg, short, full)
	require.Len(t, msg.Extra, 3)
	require.Equalf(t, extra["_a"].(int64), int64(msg.Extra["_a"].(float64)), "_a didn't roundtrip")
	require.Equalf(t, extra["_file"].(string), msg.Extra["_file"].(string), "_file didn't roundtrip")
	require.Equalf(t, extra["_line"].(int), int(msg.Extra["_line"].(float64)), "_line didn't roundtrip")
}

func TestWrite2MessagesWithConnectionDropTCP(t *testing.T) {
	msgData1 := "First message\nThis happens before the connection drops"
	msgData2 := "Second message\nThis happens after the connection drops"

	msg1, msg2 := sendAndRecv2MessagesWithDropTCP(t, msgData1, msgData2)
	assertMessages(t, msg1, "First message", msgData1)
	assertMessages(t, msg2, "Second message", msgData2)
}

func TestWrite2MessagesWithServerDropTCP(t *testing.T) {
	msgData1 := "First message\nThis happens before the server drops"
	msgData2 := "Second message\nThis happens after the server drops"

	msg1 := sendAndRecv2MessagesWithServerDropTCP(t, msgData1, msgData2)

	assertMessages(t, msg1, "First message", msgData1)
}

func setupConnections(t *testing.T) (*TCPReader, chan string, chan string, *TCPWriter) {
	t.Helper()

	r, closeSignal, doneSignal, err := newTCPReader("127.0.0.1:0")
	require.NoError(t, err)

	w, err := NewTCPWriter(r.addr())
	require.NoError(t, err)

	return r, closeSignal, doneSignal, w
}

func sendAndRecvTCP(t *testing.T, msgData string) *Message {
	t.Helper()

	r, closeSignal, doneSignal, w := setupConnections(t)

	_, err := w.Write([]byte(msgData))
	require.NoError(t, err)

	closeSignal <- "stop"
	done := <-doneSignal
	require.Equalf(t, "done", done, "Wrong signal received")

	message, err := r.readMessage()
	require.NoError(t, err)

	return message
}

func sendAndRecvMsgTCP(t *testing.T, msg *Message) *Message {
	t.Helper()

	r, closeSignal, doneSignal, w := setupConnections(t)

	require.NoError(t, w.WriteMessage(msg))

	closeSignal <- "stop"
	done := <-doneSignal
	require.Equalf(t, "done", done, "Wrong signal received")

	w.Close()
	message, err := r.readMessage()
	require.NoError(t, err)

	return message
}

func sendAndRecv2MessagesWithDropTCP(t *testing.T, msgData1 string, msgData2 string) (*Message, *Message) {
	t.Helper()

	r, closeSignal, doneSignal, w := setupConnections(t)

	_, err := w.Write([]byte(msgData1))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	closeSignal <- "drop"
	done := <-doneSignal
	require.Equalf(t, "done", done, "Wrong signal received")

	message1, err := r.readMessage()
	require.NoError(t, err)

	// Need to write twice to force the detection of the dropped connection
	_, err = w.Write([]byte(msgData2))
	require.NoErrorf(t, err, "write 1")

	time.Sleep(200 * time.Millisecond)
	_, err = w.Write([]byte(msgData2))
	require.NoErrorf(t, err, "write 2")

	time.Sleep(200 * time.Millisecond)

	closeSignal <- "stop"
	done = <-doneSignal
	require.Equalf(t, "done", done, "Wrong signal received")

	message2, err := r.readMessage()
	require.NoError(t, err)

	w.Close()
	return message1, message2
}

func sendAndRecv2MessagesWithServerDropTCP(t *testing.T, msgData1 string, msgData2 string) *Message {
	t.Helper()

	r, closeSignal, doneSignal, w := setupConnections(t)

	_, err := w.Write([]byte(msgData1))
	require.NoError(t, err)

	closeSignal <- "stop"
	done := <-doneSignal
	require.Equalf(t, "done", done, "Wrong signal received")

	message1, err := r.readMessage()
	require.NoError(t, err)

	// Need to write twice to force the detection of the dropped connection
	// The first write will not cause an error, but the subsequent ones will
	for {
		_, err = w.Write([]byte(msgData2))
		if err != nil {
			break
		}
	}

	w.Close()
	return message1
}
