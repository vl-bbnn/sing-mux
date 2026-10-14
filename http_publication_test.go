package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type publicationBody struct {
	io.Reader
	closed int32
}

func (b *publicationBody) Close() error { atomic.AddInt32(&b.closed, 1); return nil }

type publicationWriter struct {
	bytes.Buffer
	closed int32
}

func (w *publicationWriter) Close() error { atomic.AddInt32(&w.closed, 1); return nil }
func TestLateHTTPResponseMustReleaseBodyAfterEarlyClose(t *testing.T) {
	writer := &publicationWriter{}
	var canceled int32
	conn := newLateHTTPConn(writer, func() { atomic.AddInt32(&canceled, 1) })
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	body := &publicationBody{Reader: bytes.NewBufferString("late original body")}
	conn.setup(body, nil)
	if atomic.LoadInt32(&body.closed) != 1 || atomic.LoadInt32(&writer.closed) != 1 || atomic.LoadInt32(&canceled) != 1 {
		t.Fatal("late response body escaped original close ownership")
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatal("closed connection admitted late body", err)
	}
}
func TestLateHTTPResponseReadWaitsForOriginalPublication(t *testing.T) {
	conn := newLateHTTPConn(&publicationWriter{}, func() {})
	reply := make(chan error, 1)
	go func() {
		b := make([]byte, 8)
		_, err := io.ReadFull(conn, b)
		if err == nil && !bytes.Equal(b, []byte("original")) {
			err = errors.New("body identity changed")
		}
		reply <- err
	}()
	select {
	case <-reply:
		t.Fatal("read escaped unpublished response")
	case <-time.After(time.Millisecond):
	}
	conn.setup(&publicationBody{Reader: bytes.NewBufferString("original")}, nil)
	select {
	case err := <-reply:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("original reader not released")
	}
	_ = conn.Close()
}
func TestLateHTTPResponsePreservesOriginalSetupFailure(t *testing.T) {
	conn := newLateHTTPConn(&publicationWriter{}, func() {})
	conn.setup(nil, context.DeadlineExceeded)
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("original setup failure erased", err)
	}
	_ = conn.Close()
}

func TestLateHTTPReadCanceledBeforeSetupMustUnblock(t *testing.T) {
	conn := newLateHTTPConn(&publicationWriter{}, func() {})
	reply := make(chan error, 1)
	go func() { _, err := conn.Read(make([]byte, 1)); reply <- err }()
	_ = conn.Close()
	select {
	case err := <-reply:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal("original close not preserved", err)
		}
	case <-time.After(time.Second):
		conn.setup(nil, io.EOF)
		<-reply
		t.Fatal("closed pending read did not unblock")
	}
	conn.setup(nil, context.Canceled)
}
