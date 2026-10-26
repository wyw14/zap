// Copyright (c) 2021 Uber Technologies, Inc.
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

package zapcore

import (
	"sync/atomic"
	"time"
)

// DefaultClock is the default clock used by Zap in operations that require
// time. This clock uses the system clock for all operations.
var DefaultClock = systemClock{}

// Clock is a source of time for logged entries.
type Clock interface {
	// Now returns the current local time.
	Now() time.Time

	// NewTicker returns *time.Ticker that holds a channel
	// that delivers "ticks" of a clock.
	NewTicker(time.Duration) *time.Ticker
}

// systemClock implements default Clock that uses system time.
type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) NewTicker(duration time.Duration) *time.Ticker {
	return time.NewTicker(duration)
}

// ClockDiagnostics is a point-in-time snapshot of the runtime state collected
// for a logger's clock: the timestamp of the most recently written entry and
// flush (Sync) statistics for its sinks.
type ClockDiagnostics struct {
	// LastEntryTime is the Time of the most recently written entry, as
	// reported by the logger's injected Clock. The zero value means no
	// entry has been written yet. The time instant is preserved exactly;
	// the time.Location may differ from the one returned by the clock.
	LastEntryTime time.Time

	// EntriesLogged is the total number of entries written since the
	// diagnostics state was created.
	EntriesLogged uint64

	// Syncs is the total number of Sync (flush) calls observed.
	Syncs uint64

	// FailedSyncs is the number of Sync calls that returned a non-nil error.
	FailedSyncs uint64

	// LastSyncDuration is the wall-clock duration of the most recent Sync.
	// It is measured with the system clock, so it stays meaningful even
	// when the logger uses a frozen or fake injected Clock.
	LastSyncDuration time.Duration

	// LastSyncFailure classifies the error returned by the most recent Sync.
	// It is SyncOK when the most recent Sync succeeded (or none ran yet).
	LastSyncFailure SyncFailureKind

	// LastSyncError is the error message from the most recent failed Sync,
	// or empty when the most recent Sync succeeded.
	LastSyncError string
}

// ClockState collects runtime diagnostics about a logger's clock and sink
// flushes. All operations are lock-free and safe for concurrent use:
// recording uses atomic operations only, so it never blocks log writes and
// never modifies the entries passed to encoders (timestamp encoding is
// therefore unchanged).
//
// A logger built with New owns an isolated ClockState. Loggers derived with
// Named, With or WithOptions share the state of the logger they were derived
// from; loggers built with separate New calls never share state. Replacing
// the clock with WithClock keeps recording into the same ClockState.
type ClockState struct {
	lastEntryNanos atomic.Int64 // unix nanoseconds of the latest entry; 0 = none
	entriesLogged  atomic.Uint64

	syncs           atomic.Uint64
	failedSyncs     atomic.Uint64
	lastSyncNanos   atomic.Int64
	lastSyncFailure atomic.Uint32
	lastSyncError   atomic.Value // string
}

// NewClockState creates an empty, isolated ClockState.
func NewClockState() *ClockState {
	return &ClockState{}
}

// RecordEntry records that an entry carrying the given timestamp (taken from
// the injected clock) was written. The timestamp is observed only and never
// modified.
func (s *ClockState) RecordEntry(t time.Time) {
	if s == nil {
		return
	}
	s.entriesLogged.Add(1)
	s.lastEntryNanos.Store(t.UnixNano())
}

// RecordSync records a completed flush: its wall-clock duration and the error
// it returned, which may be nil.
func (s *ClockState) RecordSync(d time.Duration, err error) {
	if s == nil {
		return
	}
	s.syncs.Add(1)
	s.lastSyncNanos.Store(int64(d))
	s.lastSyncFailure.Store(uint32(ClassifySyncError(err)))
	if err == nil {
		s.lastSyncError.Store("")
		return
	}
	s.failedSyncs.Add(1)
	s.lastSyncError.Store(err.Error())
}

// Snapshot returns a best-effort, point-in-time copy of the collected
// diagnostics. It is safe to call on a nil *ClockState.
func (s *ClockState) Snapshot() ClockDiagnostics {
	if s == nil {
		return ClockDiagnostics{}
	}
	d := ClockDiagnostics{
		EntriesLogged:    s.entriesLogged.Load(),
		Syncs:            s.syncs.Load(),
		FailedSyncs:      s.failedSyncs.Load(),
		LastSyncDuration: time.Duration(s.lastSyncNanos.Load()),
		LastSyncFailure:  SyncFailureKind(s.lastSyncFailure.Load()),
	}
	if nanos := s.lastEntryNanos.Load(); nanos != 0 {
		d.LastEntryTime = time.Unix(0, nanos)
	}
	if msg, ok := s.lastSyncError.Load().(string); ok {
		d.LastSyncError = msg
	}
	return d
}
