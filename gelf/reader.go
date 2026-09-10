// Copyright 2012 SocialCode. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package gelf

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	//according to Gelf specification, all chunks must be received within 5 seconds. see:https://docs.graylog.org/docs/gelf
	maxMessageTimeout             = 5 * time.Second
	defarmentatorsCleanUpInterval = 5 * time.Second
	// maxDatagramSize is the largest payload a single UDP datagram can carry:
	// 65535 bytes minus the 8 byte UDP header and the 20 byte IPv4 header.
	// Senders are free to deliver a message that is not chunked in one datagram
	// of any size up to this limit, so the read buffer must be able to hold one
	// in full. net.UDPConn.Read discards whatever does not fit without reporting
	// an error, which would leave a truncated message that fails to decode.
	maxDatagramSize = 65507
)

type Reader struct {
	mu                     sync.Mutex
	conn                   net.Conn
	messageDefragmentators map[string]*defragmentator
	done                   chan struct{}
	maxMessageTimeout      time.Duration
	// bufPool hands out the datagram sized read buffers. Pooling them avoids
	// allocating 64 KiB for every message read, while still giving concurrent
	// callers a buffer each.
	bufPool sync.Pool
}

// NewReader creates a reader for specified addr
func NewReader(addr string) (*Reader, error) {
	return newReader(addr, maxMessageTimeout, defarmentatorsCleanUpInterval)
}

func newReader(addr string, maxMessageTimeout time.Duration, defarmentatorsCleanUpInterval time.Duration) (*Reader, error) {
	var err error
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("ResolveUDPAddr('%s'): %s", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("ListenUDP: %s", err)
	}

	r := new(Reader)
	r.maxMessageTimeout = maxMessageTimeout
	r.conn = conn
	r.messageDefragmentators = make(map[string]*defragmentator)
	r.done = make(chan struct{})
	r.bufPool = sync.Pool{New: func() interface{} {
		buf := make([]byte, maxDatagramSize)
		return &buf
	}}
	r.initDefragmentatorsCleanup(defarmentatorsCleanUpInterval)
	return r, nil
}

func (r *Reader) Addr() string {
	return r.conn.LocalAddr().String()
}

// FIXME: this will discard data if p isn't big enough to hold the
// full message.
func (r *Reader) Read(p []byte) (int, error) {
	msg, err := r.ReadMessage()
	if err != nil {
		return -1, err
	}

	var data string

	if msg.Full == "" {
		data = msg.Short
	} else {
		data = msg.Full
	}

	return strings.NewReader(data).Read(p)
}

// ReadMessage reads the message from connection.
//
// If reader receives a message that is not chunked, the reader returns it immediately.
//
// If reader receives a message that is chunked, the reader finds or creates defragmentator by messageID and adds the message to the defragmentator
// and continue reading the message from the connection until it receives the last chunk of any previously received messages or if the reader receives not chunked message.
// In this case, not chunked message will be returned immediately.
func (r *Reader) ReadMessage() (*Message, error) {
	bufPtr := r.bufPool.Get().(*[]byte)
	defer r.bufPool.Put(bufPtr)
	cBuf := *bufPtr
	var (
		err       error
		n         int
		chunkHead []byte
		cReader   io.Reader
	)
	var message []byte

	for {
		// we need to reset buffer length because we change the length of the buffer to `n` after the reading data from connection
		cBuf = cBuf[:maxDatagramSize]
		if n, err = r.conn.Read(cBuf); err != nil {
			return nil, fmt.Errorf("Read: %s", err)
		}
		// the read buffer is reused across messages, so it may still hold bytes of
		// an earlier datagram past the n bytes just read. Truncate before looking
		// at the head to keep those stale bytes out of the message.
		cBuf = cBuf[:n]
		if len(cBuf) < 2 {
			return nil, fmt.Errorf("message size must be at least %v bytes", 2)
		}
		// first two bytes contains hardcoded values [0x1e, 0x0f] if message is chunked
		chunkHead = cBuf[:2]
		if bytes.Equal(chunkHead, magicChunked) {
			if len(cBuf) <= chunkedHeaderLen {
				return nil, fmt.Errorf("chunked message size must be greather than %v", chunkedHeaderLen)
			}
			// in chunked message, message id is 8 bytes after the message head
			messageID := string(cBuf[2 : 2+8])
			deframentator := getDeframentator(r, messageID)
			if deframentator.processed >= maxChunksCount {
				return nil, fmt.Errorf("message must not be split into more than %v chunks", maxChunksCount)
			}
			if deframentator.expired() {
				return nil, fmt.Errorf("message with ID: %v is expired", messageID)
			}
			if messageCompleted := deframentator.update(cBuf); !messageCompleted {
				continue
			}
			message = deframentator.bytes()
			chunkHead = message[:2]
			r.mu.Lock()
			delete(r.messageDefragmentators, messageID)
			r.mu.Unlock()
		} else {
			message = cBuf
		}
		break
	}

	// the data we get from the wire is compressed
	if bytes.Equal(chunkHead, magicGzip) {
		cReader, err = gzip.NewReader(bytes.NewReader(message))
	} else if chunkHead[0] == magicZlib[0] &&
		(int(chunkHead[0])*256+int(chunkHead[1]))%31 == 0 {
		// zlib is slightly more complicated, but correct
		cReader, err = zlib.NewReader(bytes.NewReader(message))
	} else {
		// compliance with https://github.com/Graylog2/graylog2-server
		// treating all messages as uncompressed if  they are not gzip, zlib or
		// chunked
		cReader = bytes.NewReader(message)
	}

	if err != nil {
		return nil, fmt.Errorf("NewReader: %s", err)
	}

	msg := new(Message)
	if err := json.NewDecoder(cReader).Decode(&msg); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %s", err)
	}

	return msg, nil
}

func getDeframentator(r *Reader, messageID string) *defragmentator {
	r.mu.Lock()
	defer r.mu.Unlock()
	defragmentator, ok := r.messageDefragmentators[messageID]
	if !ok {
		defragmentator = newDefragmentator(r.maxMessageTimeout)
		r.messageDefragmentators[messageID] = defragmentator
	}
	return defragmentator
}

func (r *Reader) Close() error {
	close(r.done)
	return r.conn.Close()
}

func (r *Reader) initDefragmentatorsCleanup(defarmentatorsCleanUpInterval time.Duration) {
	go func() {
		for {
			select {
			case <-r.done:
				return
			case <-time.After(defarmentatorsCleanUpInterval):
				r.cleanUpExpiredDefragmentators()
			}
		}
	}()
}

func (r *Reader) cleanUpExpiredDefragmentators() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for messageID, defragmentator := range r.messageDefragmentators {
		if defragmentator.expired() {
			delete(r.messageDefragmentators, messageID)
		}
	}
}
