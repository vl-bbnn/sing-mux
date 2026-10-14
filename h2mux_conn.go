package mux

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/baderror"
	M "github.com/sagernet/sing/common/metadata"
)

type httpConn struct {
	access    sync.Mutex
	closed    bool
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	reader    io.Reader
	writer    io.Writer
	create    chan struct{}
	err       error
	cancel    context.CancelFunc
}

func newHTTPConn(reader io.Reader, writer io.Writer) *httpConn {
	return &httpConn{
		done:   make(chan struct{}),
		reader: reader,
		writer: writer,
	}
}

func newLateHTTPConn(writer io.Writer, cancel context.CancelFunc) *httpConn {
	return &httpConn{
		done:   make(chan struct{}),
		create: make(chan struct{}),
		writer: writer,
		cancel: cancel,
	}
}

func (c *httpConn) setup(reader io.Reader, err error) {
	c.access.Lock()
	closed := c.closed
	if closed {
		c.err = net.ErrClosed
	} else {
		c.reader = reader
		c.err = err
	}
	close(c.create)
	c.access.Unlock()
	// A response arriving after Close still belongs to this exact connection.
	if closed {
		_ = common.Close(reader)
	}
}

func (c *httpConn) Read(b []byte) (n int, err error) {
	// Always observe response publication before looking at reader/err.
	// Testing reader first races with asynchronous successful setup.
	if c.create != nil {
		select {
		case <-c.create:
		case <-c.done:
			return 0, net.ErrClosed
		}
	}
	c.access.Lock()
	reader, setupErr, closed := c.reader, c.err, c.closed
	c.access.Unlock()
	if closed {
		return 0, net.ErrClosed
	}
	if setupErr != nil {
		return 0, setupErr
	}
	n, err = reader.Read(b)
	return n, baderror.WrapH2(err)
}

func (c *httpConn) Write(b []byte) (n int, err error) {
	n, err = c.writer.Write(b)
	return n, baderror.WrapH2(err)
}

func (c *httpConn) Close() error {
	c.closeOnce.Do(func() {
		c.access.Lock()
		c.closed = true
		reader := c.reader
		if c.done != nil {
			close(c.done)
		}
		c.access.Unlock()
		if c.cancel != nil {
			c.cancel()
		}
		c.closeErr = common.Close(reader, c.writer)
	})
	return c.closeErr
}

func (c *httpConn) CloseWrite() error {
	return common.Close(c.writer)
}

func (c *httpConn) LocalAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *httpConn) RemoteAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *httpConn) SetDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *httpConn) SetReadDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *httpConn) SetWriteDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *httpConn) NeedAdditionalReadDeadline() bool {
	return true
}
