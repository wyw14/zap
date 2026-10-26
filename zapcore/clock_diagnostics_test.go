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

package zapcore

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

type timeoutTestError struct{}

func (timeoutTestError) Error() string { return "flush timed out" }
func (timeoutTestError) Timeout() bool { return true }

type temporaryTestError struct{}

func (temporaryTestError) Error() string   { return "temporarily unavailable" }
func (temporaryTestError) Temporary() bool { return true }

func TestClassifySyncError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want SyncFailureKind
	}{
		{
			name: "nil",
			err:  nil,
			want: SyncOK,
		},
		{
			name: "generic",
			err:  errors.New("boom"),
			want: SyncFailureFailed,
		},
		{
			name: "timeout",
			err:  timeoutTestError{},
			want: SyncFailureTimeout,
		},
		{
			name: "temporary",
			err:  temporaryTestError{},
			want: SyncFailureTemporary,
		},
		{
			name: "wrapped timeout",
			err:  fmt.Errorf("flush: %w", timeoutTestError{}),
			want: SyncFailureTimeout,
		},
		{
			name: "closed",
			err:  fmt.Errorf("write: %w", fs.ErrClosed),
			want: SyncFailureClosed,
		},
		{
			name: "unsupported",
			err:  errors.New("sync /dev/stdout: invalid argument"),
			want: SyncFailureUnsupported,
		},
		{
			name: "unsupported handle",
			err:  errors.New("sync: The handle is invalid."),
			want: SyncFailureUnsupported,
		},
		{
			name: "joined picks most specific",
			err:  multierr.Append(errors.New("boom"), timeoutTestError{}),
			want: SyncFailureTimeout,
		},
		{
			name: "joined generic and closed",
			err:  multierr.Append(fs.ErrClosed, errors.New("boom")),
			want: SyncFailureClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ClassifySyncError(tt.err))
		})
	}
}

func TestSyncFailureKindString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ok", SyncOK.String())
	assert.Equal(t, "failed", SyncFailureFailed.String())
	assert.Equal(t, "unsupported", SyncFailureUnsupported.String())
	assert.Equal(t, "closed", SyncFailureClosed.String())
	assert.Equal(t, "temporary", SyncFailureTemporary.String())
	assert.Equal(t, "timeout", SyncFailureTimeout.String())
	assert.Equal(t, "unknown", SyncFailureKind(42).String())
}

func TestClockStateSnapshotEmpty(t *testing.T) {
	t.Parallel()

	s := NewClockState()
	d := s.Snapshot()
	assert.True(t, d.LastEntryTime.IsZero())
	assert.Zero(t, d.EntriesLogged)
	assert.Zero(t, d.Syncs)
	assert.Zero(t, d.FailedSyncs)
	assert.Zero(t, d.LastSyncDuration)
	assert.Equal(t, SyncOK, d.LastSyncFailure)
	assert.Empty(t, d.LastSyncError)
}

func TestClockStateRecordEntry(t *testing.T) {
	t.Parallel()

	s := NewClockState()
	first := time.Date(2077, 1, 23, 10, 15, 13, 441, time.UTC)
	later := first.Add(time.Hour)

	s.RecordEntry(first)
	s.RecordEntry(later)

	d := s.Snapshot()
	assert.Equal(t, uint64(2), d.EntriesLogged)
	// The exact instant is preserved even though the location is not.
	assert.Equal(t, later.UnixNano(), d.LastEntryTime.UnixNano())
}

func TestClockStateRecordSync(t *testing.T) {
	t.Parallel()

	s := NewClockState()

	s.RecordSync(5*time.Millisecond, nil)
	d := s.Snapshot()
	assert.Equal(t, uint64(1), d.Syncs)
	assert.Zero(t, d.FailedSyncs)
	assert.Equal(t, 5*time.Millisecond, d.LastSyncDuration)
	assert.Equal(t, SyncOK, d.LastSyncFailure)
	assert.Empty(t, d.LastSyncError)

	s.RecordSync(10*time.Millisecond, timeoutTestError{})
	d = s.Snapshot()
	assert.Equal(t, uint64(2), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, 10*time.Millisecond, d.LastSyncDuration)
	assert.Equal(t, SyncFailureTimeout, d.LastSyncFailure)
	assert.Equal(t, "flush timed out", d.LastSyncError)

	// A later successful sync clears the error and classification.
	s.RecordSync(time.Millisecond, nil)
	d = s.Snapshot()
	assert.Equal(t, uint64(3), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, SyncOK, d.LastSyncFailure)
	assert.Empty(t, d.LastSyncError)
}

func TestClockStateNilSafe(t *testing.T) {
	t.Parallel()

	var s *ClockState
	assert.NotPanics(t, func() {
		s.RecordEntry(time.Now())
		s.RecordSync(time.Millisecond, nil)
	})
	assert.Equal(t, ClockDiagnostics{}, s.Snapshot())
}

type fakeDiagnosticsCore struct {
	enabled bool
	entries []Entry
	syncErr error
	syncs   int
}

func (c *fakeDiagnosticsCore) Enabled(Level) bool { return c.enabled }

func (c *fakeDiagnosticsCore) With([]Field) Core { return c }

func (c *fakeDiagnosticsCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if c.enabled {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *fakeDiagnosticsCore) Write(ent Entry, _ []Field) error {
	c.entries = append(c.entries, ent)
	return nil
}

func (c *fakeDiagnosticsCore) Sync() error {
	c.syncs++
	return c.syncErr
}

func TestInstrumentCoreRecordsEntries(t *testing.T) {
	t.Parallel()

	inner := &fakeDiagnosticsCore{enabled: true}
	state := NewClockState()
	core := InstrumentCore(inner, state)

	when := time.Date(2077, 1, 23, 10, 15, 13, 441, time.UTC)
	ent := Entry{Level: InfoLevel, Time: when, Message: "hello"}
	ce := core.Check(ent, nil)
	require.NotNil(t, ce)
	ce.Write()

	// The entry reaches the wrapped core unchanged.
	require.Len(t, inner.entries, 1)
	assert.Equal(t, when, inner.entries[0].Time)
	assert.Equal(t, "hello", inner.entries[0].Message)

	d := state.Snapshot()
	assert.Equal(t, uint64(1), d.EntriesLogged)
	assert.Equal(t, when.UnixNano(), d.LastEntryTime.UnixNano())

	// Disabled entries are neither written nor recorded.
	inner.enabled = false
	assert.Nil(t, core.Check(Entry{Level: DebugLevel, Time: when}, nil))
	assert.Equal(t, uint64(1), state.Snapshot().EntriesLogged)
}

func TestInstrumentCoreDerivedWithSharesState(t *testing.T) {
	t.Parallel()

	inner := &fakeDiagnosticsCore{enabled: true}
	state := NewClockState()
	core := InstrumentCore(inner, state).With(nil)

	ce := core.Check(Entry{Level: InfoLevel, Time: time.Unix(123, 0)}, nil)
	require.NotNil(t, ce)
	ce.Write()

	assert.Equal(t, uint64(1), state.Snapshot().EntriesLogged)
}

func TestInstrumentCoreTimesSyncAndClassifies(t *testing.T) {
	t.Parallel()

	inner := &fakeDiagnosticsCore{enabled: true, syncErr: timeoutTestError{}}
	state := NewClockState()
	core := InstrumentCore(inner, state)

	err := core.Sync()
	assert.Equal(t, timeoutTestError{}, err, "Sync error must pass through unchanged")
	assert.Equal(t, 1, inner.syncs)

	d := state.Snapshot()
	assert.Equal(t, uint64(1), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, SyncFailureTimeout, d.LastSyncFailure)
	assert.Equal(t, "flush timed out", d.LastSyncError)
}

func TestInstrumentCoreNilState(t *testing.T) {
	t.Parallel()

	inner := &fakeDiagnosticsCore{enabled: true}
	assert.Same(t, Core(inner), InstrumentCore(inner, nil))
}

type slowWriteSyncer struct {
	delay time.Duration
	err   error
}

func (w *slowWriteSyncer) Write(p []byte) (int, error) { return len(p), nil }

func (w *slowWriteSyncer) Sync() error {
	time.Sleep(w.delay)
	return w.err
}

func TestInstrumentWriteSyncerTimesSync(t *testing.T) {
	t.Parallel()

	state := NewClockState()
	ws := InstrumentWriteSyncer(&slowWriteSyncer{
		delay: 10 * time.Millisecond,
		err:   fmt.Errorf("close: %w", fs.ErrClosed),
	}, state)

	n, err := ws.Write([]byte("x"))
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	err = ws.Sync()
	require.Error(t, err)

	d := state.Snapshot()
	assert.Equal(t, uint64(1), d.Syncs)
	assert.Equal(t, uint64(1), d.FailedSyncs)
	assert.Equal(t, SyncFailureClosed, d.LastSyncFailure)
	assert.GreaterOrEqual(t, d.LastSyncDuration, 5*time.Millisecond)
}

func TestInstrumentWriteSyncerNilState(t *testing.T) {
	t.Parallel()

	raw := WriteSyncer(&slowWriteSyncer{})
	assert.Same(t, raw, InstrumentWriteSyncer(raw, nil))
}
