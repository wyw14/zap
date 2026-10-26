// Copyright (c) 2026 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestClockDiagnosticsEmpty(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(log *Logger, _ *observer.ObservedLogs) {
		d := log.ClockDiagnostics()
		assert.True(t, d.LastEntryTime.IsZero())
		assert.Zero(t, d.EntriesLogged)
		assert.Zero(t, d.Syncs)
		assert.Zero(t, d.FailedSyncs)
		assert.Equal(t, zapcore.SyncOK, d.LastSyncFailure)
		assert.Empty(t, d.LastSyncError)
	})
}

// Replacing the clock changes subsequent entry timestamps but leaves the
// encoded timestamp untouched; diagnostics accumulate in the same logger
// family and stay isolated between independently built loggers.
func TestClockDiagnosticsReplaceClock(t *testing.T) {
	date1 := time.Date(2077, 1, 23, 10, 15, 13, 441, time.UTC)
	date2 := date1.Add(2 * time.Hour)
	date3 := date2.AddDate(0, 1, 0)

	core1, logs1 := observer.New(DebugLevel)
	log1 := New(core1, WithClock(constantClock(date1)))

	log1.Info("first")
	require.Equal(t, 1, logs1.Len())
	// The encoded timestamp must be exactly what the injected clock returned.
	assert.Equal(t, date1, logs1.All()[0].Time)
	d := log1.ClockDiagnostics()
	assert.Equal(t, uint64(1), d.EntriesLogged)
	assert.Equal(t, date1.UnixNano(), d.LastEntryTime.UnixNano())

	// Replace the clock on a derived logger: new entries use the new clock,
	// while diagnostics keep accumulating in the shared family state.
	log2 := log1.WithOptions(WithClock(constantClock(date2)))
	log2.Info("second")
	require.Equal(t, 2, logs1.Len())
	assert.Equal(t, date2, logs1.All()[1].Time)
	d = log2.ClockDiagnostics()
	assert.Equal(t, uint64(2), d.EntriesLogged)
	assert.Equal(t, date2.UnixNano(), d.LastEntryTime.UnixNano())

	// The parent keeps its original clock.
	log1.Info("third")
	require.Equal(t, 3, logs1.Len())
	assert.Equal(t, date1, logs1.All()[2].Time)
	assert.Equal(t, date1.UnixNano(), log1.ClockDiagnostics().LastEntryTime.UnixNano())
	// Derived loggers observe the same family diagnostics.
	assert.Equal(t, date1.UnixNano(), log2.ClockDiagnostics().LastEntryTime.UnixNano())

	// An independently constructed logger has fully isolated clock state.
	core3, logs3 := observer.New(DebugLevel)
	log3 := New(core3, WithClock(constantClock(date3)))
	log3.Info("other")
	require.Equal(t, 1, logs3.Len())
	assert.Equal(t, date3, logs3.All()[0].Time)

	d3 := log3.ClockDiagnostics()
	assert.Equal(t, uint64(1), d3.EntriesLogged)
	assert.Equal(t, date3.UnixNano(), d3.LastEntryTime.UnixNano())

	d1 := log1.ClockDiagnostics()
	assert.Equal(t, uint64(3), d1.EntriesLogged, "loggers built with New must not share clock state")
	assert.Equal(t, date1.UnixNano(), d1.LastEntryTime.UnixNano())
}

// errFlushTimeout is the canonical timeout-classified flush failure.
type errFlushTimeout struct{}

func (errFlushTimeout) Error() string { return "flush timed out" }
func (errFlushTimeout) Timeout() bool { return true }

// asyncSink hands writes to a background worker and flushes on Sync, so
// logging does not wait for the destination and Sync drives the worker.
type asyncSink struct {
	jobs    chan []byte
	flushes chan chan error
	stop    chan struct{}

	mu      sync.Mutex
	written [][]byte
	delay   time.Duration
	err     error
}

func newAsyncSink(delay time.Duration, err error) *asyncSink {
	s := &asyncSink{
		jobs:    make(chan []byte, 64),
		flushes: make(chan chan error, 16),
		stop:    make(chan struct{}),
		delay:   delay,
		err:     err,
	}
	go s.run()
	return s
}

func (s *asyncSink) run() {
	for {
		select {
		case <-s.stop:
			return
		case p := <-s.jobs:
			s.mu.Lock()
			s.written = append(s.written, p)
			s.mu.Unlock()
		case done := <-s.flushes:
			// Simulate a background flush taking some time.
			time.Sleep(s.delay)
			s.mu.Lock()
			err := s.err
			s.mu.Unlock()
			done <- err
		}
	}
}

func (s *asyncSink) Write(p []byte) (int, error) {
	s.jobs <- append([]byte(nil), p...)
	return len(p), nil
}

func (s *asyncSink) Sync() error {
	done := make(chan error, 1)
	s.flushes <- done
	return <-done
}

func (s *asyncSink) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *asyncSink) lines() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.written))
	for i, p := range s.written {
		out[i] = append([]byte(nil), p...)
	}
	return out
}

// With an asynchronous sink, writes and diagnostics must not block on a
// flush; Sync duration is measured on the wall clock and its failure is
// classified, while encoded timestamps still come from the injected clock.
func TestClockDiagnosticsAsyncSink(t *testing.T) {
	date := time.Date(2077, 7, 1, 0, 0, 0, 0, time.UTC)
	sink := newAsyncSink(25*time.Millisecond, errFlushTimeout{})
	defer close(sink.stop)

	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		MessageKey:  "msg",
		LevelKey:    "level",
		TimeKey:     "ts",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
		EncodeTime:  zapcore.EpochTimeEncoder,
	})
	log := New(zapcore.NewCore(enc, sink, zapcore.DebugLevel), WithClock(constantClock(date)))

	// The entry is recorded without any Sync call and without blocking on
	// the background worker.
	log.Info("queued")
	require.Eventually(t, func() bool {
		return log.ClockDiagnostics().EntriesLogged == 1
	}, time.Second, time.Millisecond)
	d := log.ClockDiagnostics()
	assert.Equal(t, date.UnixNano(), d.LastEntryTime.UnixNano())
	assert.Zero(t, d.Syncs, "no Sync should have happened yet")

	// The worker receives the bytes asynchronously; the timestamp in the
	// encoded payload must be the injected clock time.
	var lines [][]byte
	require.Eventually(t, func() bool {
		lines = sink.lines()
		return len(lines) == 1
	}, time.Second, time.Millisecond)

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(lines[0], &payload))
	assert.InDelta(t, float64(date.UnixNano())/1e9, payload["ts"], 1e-9)
	assert.Equal(t, "queued", payload["msg"])

	// Failed flush: duration measured against the wall clock even though
	// the injected clock is frozen, and the error is classified as timeout.
	err := log.Sync()
	require.ErrorIs(t, err, errFlushTimeout{})
	d = log.ClockDiagnostics()
	assert.Equal(t, uint64(1), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, zapcore.SyncFailureTimeout, d.LastSyncFailure)
	assert.Contains(t, d.LastSyncError, "flush timed out")
	assert.GreaterOrEqual(t, d.LastSyncDuration, 20*time.Millisecond)

	// A later successful flush clears the recorded error.
	sink.setErr(nil)
	require.NoError(t, log.Sync())
	d = log.ClockDiagnostics()
	assert.Equal(t, uint64(2), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs, "historical failure count must be preserved")
	assert.Equal(t, zapcore.SyncOK, d.LastSyncFailure)
	assert.Empty(t, d.LastSyncError)
}

// countingWriteSyncer fails every third Sync, deterministically under a lock.
type countingWriteSyncer struct {
	mu    sync.Mutex
	syncs int
}

func (s *countingWriteSyncer) Write(p []byte) (int, error) { return len(p), nil }

func (s *countingWriteSyncer) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncs++
	if s.syncs%3 == 0 {
		return fmt.Errorf("flush: %w", fs.ErrClosed)
	}
	return nil
}

func TestClockDiagnosticsConcurrentSync(t *testing.T) {
	const goroutines = 8
	const iterations = 100

	ws := &countingWriteSyncer{}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(NewProductionEncoderConfig()),
		ws,
		zapcore.DebugLevel,
	)
	log := New(core)

	var wg sync.WaitGroup
	runConcurrently(goroutines, iterations, &wg, func() {
		_ = log.Sync()
	})
	runConcurrently(goroutines, iterations, &wg, func() {
		log.Info("concurrent")
	})
	wg.Wait()

	ws.mu.Lock()
	sinkSyncs := ws.syncs
	ws.mu.Unlock()
	assert.Equal(t, goroutines*iterations, sinkSyncs)

	d := log.ClockDiagnostics()
	assert.Equal(t, uint64(goroutines*iterations), d.Syncs)
	assert.Equal(t, uint64(goroutines*iterations), d.EntriesLogged)
	assert.Equal(t, uint64(goroutines*iterations/3), d.FailedSyncs)
}

type stubSink struct {
	written int
	closed  bool
	syncErr error
}

func (s *stubSink) Write(p []byte) (int, error) {
	s.written += len(p)
	return len(p), nil
}

func (s *stubSink) Sync() error { return s.syncErr }

func (s *stubSink) Close() error {
	s.closed = true
	return nil
}

func TestInstrumentSink(t *testing.T) {
	raw := &stubSink{syncErr: errors.New("operation not supported")}
	state := zapcore.NewClockState()
	sink := InstrumentSink(raw, state)

	n, err := sink.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, 5, raw.written)

	require.Error(t, sink.Sync())
	d := state.Snapshot()
	assert.Equal(t, uint64(1), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, zapcore.SyncFailureUnsupported, d.LastSyncFailure)

	require.NoError(t, sink.Close())
	assert.True(t, raw.closed)

	// nil state must be returned unwrapped.
	other := &stubSink{}
	assert.Same(t, Sink(other), InstrumentSink(other, nil))
}
