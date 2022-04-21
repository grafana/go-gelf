package gelf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	notChunked := createMessage("a", 10)
	notChunkedBytes := prepareUDPMessages(t, notChunked)
	//message that is sent in 2 chunks
	chunked1 := createMessage("b", ChunkSize+1)
	chunkedBytes1 := prepareUDPMessages(t, chunked1)
	//message that is sent in 3 chunks
	chunked2 := createMessage("c", ChunkSize*2)
	chunkedBytes2 := prepareUDPMessages(t, chunked2)
	//message that is sent in 129 chunks
	messageBytes3 := prepareUDPMessages(t, createMessage("d", chunkedDataLen*128))
	//message that is sent in 128 chunks
	chunked4 := createMessage("e", chunkedDataLen*127)
	chunkedBytes4 := prepareUDPMessages(t, chunked4)

	tests := map[string]struct {
		udpMessagesToSend [][]byte
		expectedMessages  []Message
		expectedErrors    []error
	}{
		"expected messages to be received even if chunked messages are received in parallel": {
			udpMessagesToSend: [][]byte{chunkedBytes1[0], chunkedBytes2[0], chunkedBytes2[1], chunkedBytes1[1], chunkedBytes2[2]},
			expectedMessages:  []Message{chunked1, chunked2},
		},
		"expected message to be received even if chunks are sent in any order": {
			udpMessagesToSend: [][]byte{chunkedBytes2[1], chunkedBytes2[0], chunkedBytes2[2]},
			expectedMessages:  []Message{chunked2},
		},
		"expected messages to be received even if not chunked message is sent between chunks": {
			udpMessagesToSend: [][]byte{chunkedBytes1[0], notChunkedBytes[0], chunkedBytes1[1]},
			expectedMessages:  []Message{chunked1, notChunked},
		},
		"expected not chunked messages to be received": {
			udpMessagesToSend: [][]byte{notChunkedBytes[0], notChunkedBytes[0]},
			expectedMessages:  []Message{notChunked, notChunked},
		},
		"expected error if chunked message length is not greater than 12 bytes": {
			udpMessagesToSend: [][]byte{magicChunked},
			expectedErrors:    []error{fmt.Errorf("chunked message size must be greather than 12")},
		},
		"expected error if message is split to more than 128 chunks": {
			udpMessagesToSend: messageBytes3,
			expectedErrors:    []error{fmt.Errorf("message must not be split into more than %v chunks", maxChunksCount)},
		},
		"expected messages to be received even if it's split into 128 chunks": {
			udpMessagesToSend: chunkedBytes4,
			expectedMessages:  []Message{chunked4},
		},
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			reader, err := NewReader(":0")
			require.NoError(t, err)
			done := make(chan bool)
			defer func() {
				_ = reader.Close()
			}()

			addr, err := net.ResolveUDPAddr("udp", reader.Addr())
			require.NoError(t, err)
			conn, err := net.DialUDP("udp", nil, addr)
			require.NoError(t, err)
			defer func() {
				_ = conn.Close()
			}()

			receivedMessages := make([]Message, 0, len(data.expectedMessages))
			errors := make([]error, 0, len(data.expectedErrors))
			messagesOutstanding := len(data.expectedMessages) + len(data.expectedErrors)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						errors = append(errors, fmt.Errorf("panic during execution: %v", r))
					}
					close(done)
				}()
				for {
					msg, err := reader.ReadMessage()
					messagesOutstanding--
					if err != nil {
						errors = append(errors, err)
					} else {
						receivedMessages = append(receivedMessages, *msg)
					}
					if messagesOutstanding == 0 {
						return
					}
				}
			}()

			for _, message := range data.udpMessagesToSend {
				if _, err := conn.Write(message); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("test deadline exceeded")
			}
			require.ElementsMatch(t, data.expectedErrors, errors)
			require.ElementsMatch(t, data.expectedMessages, receivedMessages)
		})
	}

}

func createMessage(char string, originMessageLength int) Message {
	return Message{
		Version:  "1.1",
		Host:     "hostname",
		Short:    strings.Repeat(char, originMessageLength),
		TimeUnix: float64(time.Now().UnixNano()) / float64(time.Second),
		Level:    6, // info
		Facility: "reader_test",
		Extra:    nil,
	}
}

func prepareUDPMessages(t *testing.T, message Message) [][]byte {
	enc, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) <= ChunkSize {
		return [][]byte{enc}
	}

	chunksCount := uint8(len(enc)/chunkedDataLen) + 1
	udpMessages := make([][]byte, 0, chunksCount)
	msgId := make([]byte, 8)
	if n, err := rand.Read(msgId); err != nil || len(msgId) != n {
		t.Fatal(err)
	}

	for chunk := 0; chunk*chunkedDataLen < len(enc); chunk++ {
		start := chunk * chunkedDataLen
		end := (chunk + 1) * chunkedDataLen
		if end > len(enc) {
			end = len(enc)
		}

		payload := bytes.NewBuffer([]byte{0x1e, 0x0f})
		payload.Write(msgId)
		payload.Write([]byte{uint8(chunk)})
		payload.Write([]byte{chunksCount})
		payload.Write(enc[start:end])
		udpMessages = append(udpMessages, payload.Bytes())
	}
	return udpMessages
}

func TestReaderCleaningProcess(t *testing.T) {
	maxMessageTimeout := 200 * time.Millisecond
	defarmentatorsCleanUpInterval := 50 * time.Millisecond

	reader, err := newReader(":0", maxMessageTimeout, defarmentatorsCleanUpInterval)
	require.NoError(t, err)
	defer func() {
		_ = reader.Close()
	}()

	addr, err := net.ResolveUDPAddr("udp", reader.Addr())
	require.NoError(t, err)

	conn, err := net.DialUDP("udp", nil, addr)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	notChunkedMessage := prepareUDPMessages(t, createMessage("a", 10))[0]
	//message that is split to 2 chunks
	chunks := prepareUDPMessages(t, createMessage("b", ChunkSize+1))
	// we write only one chunk so the message will be abandoned in defragmentator
	n, err := conn.Write(chunks[0])
	require.Equal(t, len(chunks[0]), n)
	require.NoError(t, err)

	// we write not chunked message to unblock the reader
	n, err = conn.Write(notChunkedMessage)
	require.NoError(t, err)
	require.Equal(t, len(notChunkedMessage), n)

	_, err = reader.ReadMessage()
	require.NoError(t, err)

	messageID := string(getMessageId(chunks[0]))
	require.Truef(t, containsDefragmentator(reader, messageID), "reader must contain defragmentator for messageID: %v", messageID)

	require.Eventually(t, func() bool {
		return !containsDefragmentator(reader, messageID)
	}, 2*time.Second, 50*time.Millisecond, "defragmentator must be removed")
}

func getMessageId(chunk []byte) []byte {
	return chunk[2:10]
}

func containsDefragmentator(reader *Reader, messageID string) bool {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	_, exists := reader.messageDefragmentators[messageID]
	return exists
}
