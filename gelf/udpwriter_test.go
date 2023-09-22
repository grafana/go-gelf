// Copyright 2012 SocialCode. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package gelf

import (
	"compress/flate"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewUDPWriter(t *testing.T) {
	w, err := NewUDPWriter("")
	if err == nil || w != nil {
		t.Errorf("New didn't fail")
		return
	}
}

func sendAndRecvRaw(t *testing.T, msgData string, compress CompressType) ([]byte, error) {
	t.Helper()

	r, err := NewReader("127.0.0.1:0")
	require.NoError(t, err)

	defer r.Close()

	w, err := NewUDPWriter(r.Addr())
	require.NoError(t, err)

	w.CompressionType = compress

	_, err = w.Write([]byte(msgData))
	require.NoError(t, err)

	w.Close()
	b := make([]byte, 512)
	i, err := r.Read(b)
	require.NoError(t, err)

	return b[:i], nil
}

func sendAndRecv(t *testing.T, msgData string, compress CompressType) *Message {
	t.Helper()

	r, err := NewReader("127.0.0.1:0")
	require.NoError(t, err)
	defer r.Close()

	w, err := NewUDPWriter(r.Addr())
	require.NoError(t, err)

	w.CompressionType = compress

	_, err = w.Write([]byte(msgData))
	require.NoError(t, err)

	w.Close()
	msg, err := r.ReadMessage()
	require.NoError(t, err)

	return msg
}

func sendAndRecvMsg(t *testing.T, msg *Message, compress CompressType) *Message {
	t.Helper()

	r, err := NewReader("127.0.0.1:0")
	require.NoError(t, err)

	defer r.Close()

	w, err := NewUDPWriter(r.Addr())
	require.NoError(t, err)

	w.CompressionType = compress

	err = w.WriteMessage(msg)
	require.NoError(t, err)

	w.Close()
	res, err := r.ReadMessage()
	require.NoError(t, err)

	return res
}

// tests read raw single-message (non-chunked) messages that are split over multiple lines
func TestReadRawSmallMultiLine(t *testing.T) {
	for _, tc := range []CompressType{CompressGzip, CompressZlib} {
		t.Run(fmt.Sprintf("CompressType: %s", tc.String()), func(t *testing.T) {
			msgData := "awesomesauce\nbananas"

			raw, err := sendAndRecvRaw(t, msgData, tc)
			require.NoError(t, err)

			require.Equal(t, msgData, string(raw))
		})
	}
}

// tests single-message (non-chunked) messages that are split over multiple lines
func TestWriteSmallMultiLine(t *testing.T) {
	for _, tc := range []CompressType{CompressGzip, CompressZlib} {
		t.Run(fmt.Sprintf("CompressType: %s", tc.String()), func(t *testing.T) {
			msgData := "awesomesauce\nbananas"

			msg := sendAndRecv(t, msgData, tc)

			assertMessages(t, msg, "awesomesauce", msgData)
		})
	}
}

// tests single-message (non-chunked) messages that are a single line long
func TestWriteSmallOneLine(t *testing.T) {
	msgData := "some awesome thing\n"
	msgDataTrunc := msgData[:len(msgData)-1]

	msg := sendAndRecv(t, msgData, CompressGzip)
	// should remove the trailing newline
	assertMessages(t, msg, msgDataTrunc, "")
	assertHasSuffix(t, msg.Extra["_file"].(string), "/go-gelf/gelf/udpwriter_test.go", "")

	require.Lenf(t, msg.Extra, 2, "expect only file and line")
}

func TestGetCaller(t *testing.T) {
	file, line := getCallerIgnoringLogMulti(1000)
	if line != 0 || file != "???" {
		t.Errorf("didn't fail 1 %s %d", file, line)
		return
	}

	file, _ = getCaller(0)
	assertHasSuffix(t, file, "/gelf/udpwriter_test.go", "not udpwriter_test.go 1")

	file, _ = getCallerIgnoringLogMulti(0)
	assertHasSuffix(t, file, "/gelf/udpwriter_test.go", "not udpwriter_test.go 2")
}

// tests single-message (chunked) messages
func TestWriteBigChunked(t *testing.T) {
	for _, tc := range []CompressType{CompressGzip, CompressZlib} {
		t.Run(fmt.Sprintf("CompressType: %s", tc.String()), func(t *testing.T) {
			randData := make([]byte, 4096)
			_, err := rand.Read(randData)
			require.NoError(t, err)

			msgData := "awesomesauce\n" + base64.StdEncoding.EncodeToString(randData)

			msg := sendAndRecv(t, msgData, tc)

			assertMessages(t, msg, "awesomesauce", msgData)
		})
	}

}

// tests messages with extra data
func TestExtraData(t *testing.T) {

	// time.Now().Unix() seems fine, UnixNano() won't roundtrip
	// through string -> float64 -> int64
	extra := map[string]interface{}{
		"_a":    10 * time.Now().Unix(),
		"C":     9,
		"_file": "udpwriter_test.go",
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
		Facility: "udpwriter_test",
		Extra:    extra,
		RawExtra: []byte(`{"woo": "hoo"}`),
	}

	for _, tc := range []CompressType{CompressGzip, CompressZlib} {
		t.Run(fmt.Sprintf("CompressType: %s", tc.String()), func(t *testing.T) {
			msg := sendAndRecvMsg(t, &m, tc)

			assertMessages(t, msg, short, full)
			require.Len(t, msg.Extra, 3)
			require.Equalf(t, extra["_a"].(int64), int64(msg.Extra["_a"].(float64)), "_a didn't roundtrip")
			require.Equalf(t, extra["_file"].(string), msg.Extra["_file"].(string), "_file didn't roundtrip")
			require.Equalf(t, extra["_line"].(int), int(msg.Extra["_line"].(float64)), "_line didn't roundtrip")
		})
	}
}

func BenchmarkWriteBestSpeed(b *testing.B) {
	r, err := NewReader("127.0.0.1:0")
	require.NoError(b, err)

	defer r.Close()

	go io.Copy(ioutil.Discard, r)
	w, err := NewUDPWriter(r.Addr())
	require.NoError(b, err)

	w.CompressionLevel = flate.BestSpeed
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteMessage(&Message{
			Version:  "1.1",
			Host:     w.hostname,
			Short:    "short message",
			Full:     "full message",
			TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
			Level:    6, // info
			Facility: w.Facility,
			Extra:    map[string]interface{}{"_file": "1234", "_line": "3456"},
		})
	}
}

func BenchmarkWriteNoCompression(b *testing.B) {
	r, err := NewReader("127.0.0.1:0")
	require.NoError(b, err)

	defer r.Close()

	go io.Copy(ioutil.Discard, r)
	w, err := NewUDPWriter(r.Addr())
	require.NoError(b, err)

	w.CompressionLevel = flate.NoCompression
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteMessage(&Message{
			Version:  "1.1",
			Host:     w.hostname,
			Short:    "short message",
			Full:     "full message",
			TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
			Level:    6, // info
			Facility: w.Facility,
			Extra:    map[string]interface{}{"_file": "1234", "_line": "3456"},
		})
	}
}

func BenchmarkWriteDisableCompressionCompletely(b *testing.B) {
	r, err := NewReader("127.0.0.1:0")
	require.NoError(b, err)

	defer r.Close()

	go io.Copy(ioutil.Discard, r)
	w, err := NewUDPWriter(r.Addr())
	require.NoError(b, err)

	w.CompressionType = CompressNone
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteMessage(&Message{
			Version:  "1.1",
			Host:     w.hostname,
			Short:    "short message",
			Full:     "full message",
			TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
			Level:    6, // info
			Facility: w.Facility,
			Extra:    map[string]interface{}{"_file": "1234", "_line": "3456"},
		})
	}
}

func BenchmarkWriteDisableCompressionAndPreencodeExtra(b *testing.B) {
	r, err := NewReader("127.0.0.1:0")
	require.NoError(b, err)

	defer r.Close()

	go io.Copy(ioutil.Discard, r)
	w, err := NewUDPWriter(r.Addr())
	require.NoError(b, err)

	w.CompressionType = CompressNone
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteMessage(&Message{
			Version:  "1.1",
			Host:     w.hostname,
			Short:    "short message",
			Full:     "full message",
			TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
			Level:    6, // info
			Facility: w.Facility,
			RawExtra: json.RawMessage(`{"_file":"1234","_line": "3456"}`),
		})
	}
}
