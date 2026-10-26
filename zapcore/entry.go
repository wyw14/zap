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

package zapcore

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"strings"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap/internal/bufferpool"
	"go.uber.org/zap/internal/exit"
	"go.uber.org/zap/internal/pool"
)

var _cePool = pool.New(func() *CheckedEntry {
	// Pre-allocate some space for cores.
	return &CheckedEntry{
		cores: make([]Core, 4),
	}
})

func getCheckedEntry() *CheckedEntry {
	ce := _cePool.Get()
	ce.reset()
	return ce
}

func putCheckedEntry(ce *CheckedEntry) {
	if ce == nil {
		return
	}
	_cePool.Put(ce)
}

// NewEntryCaller makes an EntryCaller from the return signature of
// runtime.Caller.
func NewEntryCaller(pc uintptr, file string, line int, ok bool) EntryCaller {
	if !ok {
		return EntryCaller{}
	}
	return EntryCaller{
		PC:      pc,
		File:    file,
		Line:    line,
		Defined: true,
	}
}

// EntryCaller represents the caller of a logging function.
type EntryCaller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

// String returns the full path and line number of the caller.
func (ec EntryCaller) String() string {
	return ec.FullPath()
}

// FullPath returns a /full/path/to/package/file:line description of the
// caller.
func (ec EntryCaller) FullPath() string {
	if !ec.Defined {
		return "undefined"
	}
	buf := bufferpool.Get()
	buf.AppendString(ec.File)
	buf.AppendByte(':')
	buf.AppendInt(int64(ec.Line))
	caller := buf.String()
	buf.Free()
	return caller
}

// TrimmedPath returns a package/file:line description of the caller,
// preserving only the leaf directory name and file name.
func (ec EntryCaller) TrimmedPath() string {
	if !ec.Defined {
		return "undefined"
	}
	// nb. To make sure we trim the path correctly on Windows too, we
	// counter-intuitively need to use '/' and *not* os.PathSeparator here,
	// because the path given originates from Go stdlib, specifically
	// runtime.Caller() which (as of Mar/17) returns forward slashes even on
	// Windows.
	//
	// See https://github.com/golang/go/issues/3335
	// and https://github.com/golang/go/issues/18151
	//
	// for discussion on the issue on Go side.
	//
	// Find the last separator.
	//
	idx := strings.LastIndexByte(ec.File, '/')
	if idx == -1 {
		return ec.FullPath()
	}
	// Find the penultimate separator.
	idx = strings.LastIndexByte(ec.File[:idx], '/')
	if idx == -1 {
		return ec.FullPath()
	}
	buf := bufferpool.Get()
	// Keep everything after the penultimate separator.
	buf.AppendString(ec.File[idx+1:])
	buf.AppendByte(':')
	buf.AppendInt(int64(ec.Line))
	caller := buf.String()
	buf.Free()
	return caller
}

// An Entry represents a complete log message. The entry's structured context
// is already serialized, but the log level, time, message, and call site
// information are available for inspection and modification. Any fields left
// empty will be omitted when encoding.
//
// Entries are pooled, so any functions that accept them MUST be careful not to
// retain references to them.
type Entry struct {
	Level      Level
	Time       time.Time
	LoggerName string
	Message    string
	Caller     EntryCaller
	Stack      string
}

// CheckWriteHook is a custom action that may be executed after an entry is
// written.
//
// Register one on a CheckedEntry with the After method.
//
//	if ce := logger.Check(...); ce != nil {
//	  ce = ce.After(hook)
//	  ce.Write(...)
//	}
//
// You can configure the hook for Fatal log statements at the logger level with
// the zap.WithFatalHook option.
type CheckWriteHook interface {
	// OnWrite is invoked with the CheckedEntry that was written and a list
	// of fields added with that entry.
	//
	// The list of fields DOES NOT include fields that were already added
	// to the logger with the With method.
	OnWrite(*CheckedEntry, []Field)
}

// CheckWriteAction indicates what action to take after a log entry is
// processed. Actions are ordered in increasing severity.
type CheckWriteAction uint8

const (
	// WriteThenNoop indicates that nothing special needs to be done. It's the
	// default behavior.
	WriteThenNoop CheckWriteAction = iota
	// WriteThenGoexit runs runtime.Goexit after Write.
	WriteThenGoexit
	// WriteThenPanic causes a panic after Write.
	WriteThenPanic
	// WriteThenFatal causes an os.Exit(1) after Write.
	WriteThenFatal
)

// OnWrite implements the OnWrite method to keep CheckWriteAction compatible
// with the new CheckWriteHook interface which deprecates CheckWriteAction.
func (a CheckWriteAction) OnWrite(ce *CheckedEntry, _ []Field) {
	switch a {
	case WriteThenGoexit:
		runtime.Goexit()
	case WriteThenPanic:
		panic(ce.Message)
	case WriteThenFatal:
		exit.With(1)
	}
}

var _ CheckWriteHook = CheckWriteAction(0)

// CheckPreWriteHook is a function that transforms an Entry and its Fields
// before they are written to cores. Register one on a CheckedEntry with the
// Before method.
//
// Pre-write hooks run in the order they were added, before any Core's Write
// method is called. They may modify the Entry and Fields freely.
type CheckPreWriteHook func(Entry, []Field) (Entry, []Field)

// CheckedEntry is an Entry together with a collection of Cores that have
// already agreed to log it.
//
// CheckedEntry references should be created by calling AddCore or After on a
// nil *CheckedEntry. References are returned to a pool after Write, and MUST
// NOT be retained after calling their Write method.
type CheckedEntry struct {
	Entry
	ErrorOutput WriteSyncer
	dirty       bool // best-effort detection of pool misuse
	after       CheckWriteHook
	cores       []Core
	before      []CheckPreWriteHook
}

func (ce *CheckedEntry) reset() {
	ce.Entry = Entry{}
	ce.ErrorOutput = nil
	ce.dirty = false
	ce.after = nil
	for i := range ce.cores {
		// don't keep references to cores
		ce.cores[i] = nil
	}
	ce.cores = ce.cores[:0]
	for i := range ce.before {
		ce.before[i] = nil
	}
	ce.before = ce.before[:0]
}

// Write writes the entry to the stored Cores, returns any errors, and returns
// the CheckedEntry reference to a pool for immediate re-use. Finally, it
// executes any required CheckWriteAction.
func (ce *CheckedEntry) Write(fields ...Field) {
	if ce == nil {
		return
	}

	if ce.dirty {
		if ce.ErrorOutput != nil {
			// Make a best effort to detect unsafe re-use of this CheckedEntry.
			// If the entry is dirty, log an internal error; because the
			// CheckedEntry is being used after it was returned to the pool,
			// the message may be an amalgamation from multiple call sites.
			_, _ = fmt.Fprintf(
				ce.ErrorOutput,
				"%v Unsafe CheckedEntry re-use near Entry %+v.\n",
				ce.Time,
				ce.Entry,
			)
			_ = ce.ErrorOutput.Sync() // ignore error
		}
		return
	}
	ce.dirty = true

	ent := ce.Entry
	for i := range ce.before {
		ent, fields = ce.before[i](ent, fields)
	}

	var err error
	for i := range ce.cores {
		err = multierr.Append(err, ce.cores[i].Write(ent, fields))
	}
	if err != nil && ce.ErrorOutput != nil {
		_, _ = fmt.Fprintf(
			ce.ErrorOutput,
			"%v write error: %v\n",
			ce.Time,
			err,
		)
		_ = ce.ErrorOutput.Sync() // ignore error
	}

	hook := ce.after
	if hook != nil {
		hook.OnWrite(ce, fields)
	}
	putCheckedEntry(ce)
}

// AddCore adds a Core that has agreed to log this CheckedEntry. It's intended to be
// used by Core.Check implementations, and is safe to call on nil CheckedEntry
// references.
func (ce *CheckedEntry) AddCore(ent Entry, core Core) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.cores = append(ce.cores, core)
	return ce
}

// Should sets this CheckedEntry's CheckWriteAction, which controls whether a
// Core will panic or fatal after writing this log entry. Like AddCore, it's
// safe to call on nil CheckedEntry references.
//
// Deprecated: Use [CheckedEntry.After] instead.
func (ce *CheckedEntry) Should(ent Entry, should CheckWriteAction) *CheckedEntry {
	return ce.After(ent, should)
}

// Before adds a pre-write hook that transforms the Entry and Fields before
// they are written to any registered Cores. Multiple hooks run in the order
// they were added. It's safe to call this on nil CheckedEntry references.
func (ce *CheckedEntry) Before(ent Entry, hook CheckPreWriteHook) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.before = append(ce.before, hook)
	return ce
}

// After sets this CheckEntry's CheckWriteHook, which will be called after this
// log entry has been written. It's safe to call this on nil CheckedEntry
// references.
func (ce *CheckedEntry) After(ent Entry, hook CheckWriteHook) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.after = hook
	return ce
}

// SyncFailureKind classifies the outcome of a WriteSyncer.Sync (flush) call.
//
// Kinds are ordered from least to most specific, so that when a joined error
// contains several failures the most useful classification wins.
type SyncFailureKind uint8

const (
	// SyncOK means the flush succeeded (the error was nil).
	SyncOK SyncFailureKind = iota

	// SyncFailureFailed is the catch-all for errors that could not be
	// classified more specifically.
	SyncFailureFailed

	// SyncFailureUnsupported means flushing is not supported by the
	// destination, such as ENOTSUP/EINVAL on special files or an invalid
	// handle (a common case when Syncing standard streams).
	SyncFailureUnsupported

	// SyncFailureClosed means the destination was closed: the error wraps
	// fs.ErrClosed (e.g. os.ErrClosed).
	SyncFailureClosed

	// SyncFailureTemporary means the error reports a recoverable temporary
	// failure: it implements "Temporary() bool" and reports true.
	SyncFailureTemporary

	// SyncFailureTimeout means the error reports a timeout: it implements
	// "Timeout() bool" and reports true.
	SyncFailureTimeout
)

// String returns a short, stable name for the classification.
func (k SyncFailureKind) String() string {
	switch k {
	case SyncOK:
		return "ok"
	case SyncFailureFailed:
		return "failed"
	case SyncFailureUnsupported:
		return "unsupported"
	case SyncFailureClosed:
		return "closed"
	case SyncFailureTemporary:
		return "temporary"
	case SyncFailureTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// ClassifySyncError classifies the error returned from WriteSyncer.Sync.
//
// Joined errors (including errors.Join and go.uber.org/multierr) are
// inspected individually and the most specific classification found is
// returned. Classification is best-effort and never changes the error.
func ClassifySyncError(err error) SyncFailureKind {
	if err == nil {
		return SyncOK
	}
	kind := SyncFailureFailed
	walkSyncErrors(err, func(e error) {
		if k := classifySyncError(e); k > kind {
			kind = k
		}
	})
	return kind
}

func classifySyncError(err error) SyncFailureKind {
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return SyncFailureTimeout
	}

	var temporaryErr interface{ Temporary() bool }
	if errors.As(err, &temporaryErr) && temporaryErr.Temporary() {
		return SyncFailureTemporary
	}

	if errors.Is(err, fs.ErrClosed) {
		return SyncFailureClosed
	}

	// Many platforms surface unsupported flushes (ENOTSUP, EINVAL, invalid
	// handles on special files) as opaque errno strings, so fall back to a
	// conservative message match.
	switch msg := strings.ToLower(err.Error()); {
	case strings.Contains(msg, "not supported"),
		strings.Contains(msg, "invalid argument"),
		strings.Contains(msg, "invalid handle"),
		strings.Contains(msg, "handle is invalid"):
		return SyncFailureUnsupported
	}

	return SyncFailureFailed
}

// walkSyncErrors visits err and every error joined or wrapped beneath it,
// including errors implementing Unwrap() error or Unwrap() []error.
func walkSyncErrors(err error, visit func(error)) {
	seen := make(map[error]bool)
	var walk func(error)
	walk = func(e error) {
		if e == nil || seen[e] {
			return
		}
		seen[e] = true
		visit(e)
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range u.Unwrap() {
				walk(child)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
}

// InstrumentCore wraps core so that every entry it actually writes and every
// Sync (flush) call it makes are reported to state. Entries are passed to the
// wrapped core and its encoders unchanged, so timestamp encoding is not
// affected, and recording uses atomic operations only, so it never blocks
// log writes.
//
// Sync durations are measured with the wall clock rather than any injected
// Clock, so they remain meaningful with frozen or fake clocks. The error
// returned by Sync is passed through untouched.
//
// If state is nil, core is returned unchanged.
func InstrumentCore(core Core, state *ClockState) Core {
	if state == nil {
		return core
	}
	return &clockDiagnosticsCore{Core: core, state: state}
}

type clockDiagnosticsCore struct {
	Core
	state *ClockState
}

var (
	_ Core           = (*clockDiagnosticsCore)(nil)
	_ leveledEnabler = (*clockDiagnosticsCore)(nil)
)

func (c *clockDiagnosticsCore) Level() Level {
	return LevelOf(c.Core)
}

func (c *clockDiagnosticsCore) With(fields []Field) Core {
	return &clockDiagnosticsCore{Core: c.Core.With(fields), state: c.state}
}

func (c *clockDiagnosticsCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	// Let the wrapped core register itself and write first, exactly like
	// RegisterHooks does; our Write only records diagnostics and must not
	// write the entry a second time.
	if downstream := c.Core.Check(ent, ce); downstream != nil {
		return downstream.AddCore(ent, c)
	}
	return ce
}

func (c *clockDiagnosticsCore) Write(ent Entry, _ []Field) error {
	c.state.RecordEntry(ent.Time)
	return nil
}

func (c *clockDiagnosticsCore) Sync() error {
	start := time.Now()
	err := c.Core.Sync()
	c.state.RecordSync(time.Since(start), err)
	return err
}
