// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap/zapcore"
)

// errDiagSinkIO is a stand-in for an unrecoverable failure reported by an
// underlying destination (e.g. a broken pipe or a failed disk write).
var errDiagSinkIO = errors.New("underlying flush I/O failure")

// diagMemorySink is an in-memory Sink test double that records write, flush
// (Sync) and close activity. It can be configured to return specific errors so
// that Sync callers can distinguish between a sink that is already closed, a
// permission problem and an underlying I/O failure.
type diagMemorySink struct {
	mu sync.Mutex

	chunks     [][]byte // copied payloads of every successful Write
	syncCalls  int      // number of Sync (flush) invocations
	closeCalls int      // number of Close invocations

	flushErr error // returned by Sync while the sink is still open
	closeErr error // returned by the first Close call

	closed bool
}

func newDiagMemorySink() *diagMemorySink {
	return &diagMemorySink{}
}

// SetFlushError configures the error returned by Sync while the sink is open.
func (s *diagMemorySink) SetFlushError(err error) {
	s.mu.Lock()
	s.flushErr = err
	s.mu.Unlock()
}

// SetCloseError configures the error returned by Close.
func (s *diagMemorySink) SetCloseError(err error) {
	s.mu.Lock()
	s.closeErr = err
	s.mu.Unlock()
}

// Write implements io.Writer. Writing to a closed sink reports os.ErrClosed.
func (s *diagMemorySink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("memory sink: write on already closed sink: %w", os.ErrClosed)
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	s.chunks = append(s.chunks, cp)
	return len(p), nil
}

// Sync implements zapcore.WriteSyncer. It records every flush invocation and
// returns the configured flush error, or an os.ErrClosed-classified error once
// the sink has been closed.
func (s *diagMemorySink) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncCalls++
	if s.closed {
		return fmt.Errorf("memory sink: flush on already closed sink: %w", os.ErrClosed)
	}
	return s.flushErr
}

// Close implements io.Closer. Closing an already-closed sink is reported as
// os.ErrClosed.
func (s *diagMemorySink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	if s.closed {
		return fmt.Errorf("memory sink: sink already closed: %w", os.ErrClosed)
	}
	s.closed = true
	return s.closeErr
}

// Bytes returns the bytes written to the sink so far.
func (s *diagMemorySink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Join(s.chunks, nil)
}

// Lines returns the written contents split on newlines, dropping the trailing
// empty segment produced by the final newline.
func (s *diagMemorySink) Lines() []string {
	output := strings.Split(string(s.Bytes()), "\n")
	return output[:len(output)-1]
}

// FlushCalls reports how many times Sync was invoked.
func (s *diagMemorySink) FlushCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncCalls
}

// CloseCalls reports how many times Close was invoked.
func (s *diagMemorySink) CloseCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

// Closed reports whether the sink has been closed.
func (s *diagMemorySink) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func diagSinkFactory(sink *diagMemorySink) func(*url.URL) (Sink, error) {
	return func(*url.URL) (Sink, error) {
		return sink, nil
	}
}

func registerDiagSink(t testing.TB, scheme string, sink *diagMemorySink) {
	t.Helper()
	require.NoError(t, RegisterSink(scheme, diagSinkFactory(sink)))
}

func stubSinkRegistry(t testing.TB) *sinkRegistry {
	origSinkRegistry := _sinkRegistry
	t.Cleanup(func() {
		_sinkRegistry = origSinkRegistry
	})

	r := newSinkRegistry()
	_sinkRegistry = r
	return r
}

func TestRegisterSink(t *testing.T) {
	stubSinkRegistry(t)

	const (
		memScheme = "mem"
		nopScheme = "no-op.1234"
	)
	var memCalls, nopCalls int

	buf := bytes.NewBuffer(nil)
	memFactory := func(u *url.URL) (Sink, error) {
		assert.Equal(t, u.Scheme, memScheme, "Scheme didn't match registration.")
		memCalls++
		return nopCloserSink{zapcore.AddSync(buf)}, nil
	}
	nopFactory := func(u *url.URL) (Sink, error) {
		assert.Equal(t, u.Scheme, nopScheme, "Scheme didn't match registration.")
		nopCalls++
		return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
	}

	require.NoError(t, RegisterSink(strings.ToUpper(memScheme), memFactory), "Failed to register scheme %q.", memScheme)
	require.NoError(t, RegisterSink(nopScheme, nopFactory), "Failed to register scheme %q.", nopScheme)

	sink, closeSink, err := Open(
		memScheme+"://somewhere",
		nopScheme+"://somewhere-else",
	)
	require.NoError(t, err, "Unexpected error opening URLs with registered schemes.")
	defer closeSink()

	assert.Equal(t, 1, memCalls, "Unexpected number of calls to memory factory.")
	assert.Equal(t, 1, nopCalls, "Unexpected number of calls to no-op factory.")

	_, err = sink.Write([]byte("foo"))
	assert.NoError(t, err, "Failed to write to combined WriteSyncer.")
	assert.Equal(t, "foo", buf.String(), "Unexpected buffer contents.")
}

func TestRegisterSinkErrors(t *testing.T) {
	nopFactory := func(_ *url.URL) (Sink, error) {
		return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
	}
	tests := []struct {
		scheme string
		err    string
	}{
		{"", "empty string"},
		{"FILE", "already registered"},
		{"42", "not a valid scheme"},
		{"http*", "not a valid scheme"},
	}

	for _, tt := range tests {
		t.Run("scheme-"+tt.scheme, func(t *testing.T) {
			r := newSinkRegistry()
			err := r.RegisterSink(tt.scheme, nopFactory)
			assert.ErrorContains(t, err, tt.err)
		})
	}
}

func TestMemorySinkDiagnostics(t *testing.T) {
	stubSinkRegistry(t)

	sink := newDiagMemorySink()
	registerDiagSink(t, "mem", sink)

	ws, closeAll, err := Open("mem://diagnostics")
	require.NoError(t, err, "Unexpected error opening the memory sink.")

	_, err = ws.Write([]byte("first\n"))
	require.NoError(t, err)
	_, err = ws.Write([]byte("second\n"))
	require.NoError(t, err)
	assert.Equal(t, "first\nsecond\n", string(sink.Bytes()), "Unexpected bytes captured by the memory sink.")
	assert.Equal(t, []string{"first", "second"}, sink.Lines(), "Unexpected lines captured by the memory sink.")

	require.NoError(t, ws.Sync(), "Healthy sink should flush without an error.")
	require.NoError(t, ws.Sync(), "Healthy sink should flush without an error.")
	assert.Equal(t, 2, sink.FlushCalls(), "Expected every Sync call to be recorded.")
	assert.False(t, sink.Closed(), "Sink should still be open before closeAll.")

	closeAll()
	assert.True(t, sink.Closed(), "closeAll should close the sink.")
	assert.Equal(t, 1, sink.CloseCalls(), "Expected the sink to be closed exactly once.")

	// Sync and Write after close must report an os.ErrClosed-classified cause.
	err = ws.Sync()
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrClosed), "Expected Sync after close to wrap os.ErrClosed, got %v.", err)

	_, err = ws.Write([]byte("third\n"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrClosed), "Expected Write after close to wrap os.ErrClosed, got %v.", err)
	assert.NotContains(t, string(sink.Bytes()), "third", "Closed sink should discard further writes.")

	// Closing again is an abnormal double-close.
	err = sink.Close()
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrClosed), "Expected double close to wrap os.ErrClosed, got %v.", err)
	assert.Equal(t, 2, sink.CloseCalls(), "Expected the second close to be recorded.")
}

func TestMemorySinkSyncFailureCauses(t *testing.T) {
	stubSinkRegistry(t)

	closedSink := newDiagMemorySink()
	registerDiagSink(t, "mem-closed", closedSink)
	require.NoError(t, closedSink.Close())

	permSink := newDiagMemorySink()
	permSink.SetFlushError(fmt.Errorf("flush: permission denied: %w", os.ErrPermission))
	registerDiagSink(t, "mem-perm", permSink)

	ioSink := newDiagMemorySink()
	ioSink.SetFlushError(fmt.Errorf("flush: disk unavailable: %w", errDiagSinkIO))
	registerDiagSink(t, "mem-io", ioSink)

	tests := []struct {
		name    string
		path    string
		wantErr error
		notErrs []error
	}{
		{
			name:    "already closed",
			path:    "mem-closed://x",
			wantErr: os.ErrClosed,
			notErrs: []error{os.ErrPermission, errDiagSinkIO},
		},
		{
			name:    "permission denied",
			path:    "mem-perm://x",
			wantErr: os.ErrPermission,
			notErrs: []error{os.ErrClosed, errDiagSinkIO},
		},
		{
			name:    "underlying I/O",
			path:    "mem-io://x",
			wantErr: errDiagSinkIO,
			notErrs: []error{os.ErrClosed, os.ErrPermission},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws, closeAll, err := Open(tt.path)
			require.NoError(t, err)
			defer closeAll()

			err = ws.Sync()
			require.Error(t, err, "Expected flush to fail.")
			assert.True(t, errors.Is(err, tt.wantErr), "Expected Sync error to wrap %v, got %v.", tt.wantErr, err)
			for _, notErr := range tt.notErrs {
				assert.False(t, errors.Is(err, notErr), "Sync error should not be classified as %v, got %v.", notErr, err)
			}
		})
	}

	// Multiple failing sinks must preserve every underlying cause. Fresh
	// instances are used here because the subtests closed the shared sinks.
	joinPerm := newDiagMemorySink()
	joinPerm.SetFlushError(fmt.Errorf("flush: permission denied: %w", os.ErrPermission))
	registerDiagSink(t, "mem-join-perm", joinPerm)
	joinIO := newDiagMemorySink()
	joinIO.SetFlushError(fmt.Errorf("flush: disk unavailable: %w", errDiagSinkIO))
	registerDiagSink(t, "mem-join-io", joinIO)

	ws, closeAll, err := Open("mem-join-perm://x", "mem-join-io://y")
	require.NoError(t, err)
	defer closeAll()

	err = ws.Sync()
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrPermission), "Expected joined flush error to retain permission cause, got %v.", err)
	assert.True(t, errors.Is(err, errDiagSinkIO), "Expected joined flush error to retain I/O cause, got %v.", err)
	assert.False(t, errors.Is(err, os.ErrClosed), "Joined flush error should not look like a closed sink, got %v.", err)
}

func TestOpenConcurrentWrites(t *testing.T) {
	stubSinkRegistry(t)

	sink := newDiagMemorySink()
	registerDiagSink(t, "mem", sink)

	ws, closeAll, err := Open("mem://concurrent")
	require.NoError(t, err)
	defer closeAll()

	const goroutines, iterations = 32, 100
	payload := []byte("x\n")

	var wg sync.WaitGroup
	runConcurrently(goroutines, iterations, &wg, func() {
		_, err := ws.Write(payload)
		assert.NoError(t, err, "Concurrent write failed.")
	})
	wg.Wait()

	lines := sink.Lines()
	require.Len(t, lines, goroutines*iterations, "Expected every concurrent write to be recorded.")
	for _, line := range lines {
		assert.Equal(t, "x", line, "Expected no interleaving or corruption in locked sink writes.")
	}

	require.NoError(t, ws.Sync())
	assert.Equal(t, 1, sink.FlushCalls(), "Expected a single flush after concurrent writes.")
}

func TestSinkReplacement(t *testing.T) {
	// First registry routes "mem" to sink A.
	stubSinkRegistry(t)
	sinkA := newDiagMemorySink()
	registerDiagSink(t, "mem", sinkA)

	wsA, closeA, err := Open("mem://a")
	require.NoError(t, err)
	defer closeA()
	_, err = wsA.Write([]byte("via A\n"))
	require.NoError(t, err)

	// Replacing the registry swaps the factory behind the same scheme.
	stubSinkRegistry(t)
	sinkB := newDiagMemorySink()
	registerDiagSink(t, "mem", sinkB)

	// Re-registering a scheme on the same registry is not allowed: callers
	// must replace the whole registry to swap a sink factory.
	assert.ErrorContains(
		t,
		RegisterSink("mem", diagSinkFactory(newDiagMemorySink())),
		"already registered",
	)

	wsB, closeB, err := Open("mem://b")
	require.NoError(t, err)
	defer closeB()
	_, err = wsB.Write([]byte("via B\n"))
	require.NoError(t, err)

	assert.Equal(t, []string{"via A"}, sinkA.Lines(), "Writes before the replacement should stay on sink A.")
	assert.Equal(t, []string{"via B"}, sinkB.Lines(), "Writes after the replacement should land on sink B.")

	// The replaced destination keeps working independently.
	require.NoError(t, wsB.Sync())
	assert.Equal(t, 1, sinkB.FlushCalls(), "Expected the flush to reach the replacement sink.")
	assert.Equal(t, 0, sinkA.FlushCalls(), "Expected the replaced sink not to receive new flushes.")
}

func TestOpenAbnormalCloseError(t *testing.T) {
	stubSinkRegistry(t)

	closeErr := errors.New("close interrupted by I/O failure")
	sink := newDiagMemorySink()
	sink.SetCloseError(closeErr)
	registerDiagSink(t, "mem", sink)

	ws, _, err := Open("mem://abnormal")
	require.NoError(t, err)

	_, err = ws.Write([]byte("before close\n"))
	require.NoError(t, err)

	// An abnormal Close surfaces its configured cause directly.
	require.ErrorIs(t, sink.Close(), closeErr)
	assert.True(t, sink.Closed(), "Sink should be marked closed even when Close fails.")

	// Subsequent flushes are classified as "already closed".
	err = ws.Sync()
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrClosed), "Expected flush after failed close to wrap os.ErrClosed, got %v.", err)
}
