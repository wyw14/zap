// Copyright (c) 2016-2022 Uber Technologies, Inc.
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

const (
	_numLevels        = _maxLevel - _minLevel + 1
	_countersPerLevel = 4096
)

type counter struct {
	resetAt atomic.Int64
	counter atomic.Uint64
}

type counters [_numLevels][_countersPerLevel]counter

func newCounters() *counters {
	return &counters{}
}

func (cs *counters) get(lvl Level, key string) *counter {
	i := lvl - _minLevel
	j := fnv32a(key) % _countersPerLevel
	return &cs[i][j]
}

// fnv32a, adapted from "hash/fnv", but without a []byte(string) alloc
func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}

// IncCheckReset increments the counter for the current sampling window,
// resetting it (and the window boundary) if the window has elapsed.
//
// It returns the post-increment count and whether this call performed the
// window reset. At most one concurrent caller reports a reset for a given
// window transition.
func (c *counter) IncCheckReset(t time.Time, tick time.Duration) (uint64, bool) {
	tn := t.UnixNano()
	resetAfter := c.resetAt.Load()
	if resetAfter > tn {
		return c.counter.Add(1), false
	}

	c.counter.Store(1)

	newResetAfter := tn + tick.Nanoseconds()
	if !c.resetAt.CompareAndSwap(resetAfter, newResetAfter) {
		// We raced with another goroutine trying to reset, and it also reset
		// the counter to 1, so we need to reincrement the counter.
		return c.counter.Add(1), false
	}

	return 1, true
}

// SamplingDecision is a decision represented as a bit field made by sampler.
// More decisions may be added in the future.
type SamplingDecision uint32

const (
	// LogDropped indicates that the Sampler dropped a log entry.
	LogDropped SamplingDecision = 1 << iota
	// LogSampled indicates that the Sampler sampled a log entry.
	LogSampled
)

// SamplerLevelStats is a point-in-time snapshot of a Sampler's runtime
// counters for a single log level.
type SamplerLevelStats struct {
	// Level these counters belong to.
	Level Level
	// Accepted is the number of entries at this level the sampler let
	// through.
	Accepted uint64
	// Dropped is the number of entries at this level the sampler dropped.
	Dropped uint64
	// Resets is the number of times a sampling window was reset at this
	// level, i.e. how many times the first entry of a new tick window was
	// observed.
	Resets uint64
}

// SamplerConfigSnapshot is a point-in-time snapshot of a Sampler's
// configuration.
type SamplerConfigSnapshot struct {
	// Tick is the length of each sampling window.
	Tick time.Duration
	// First is the number of entries with a given level and message that
	// are logged per window before sampling kicks in.
	First uint64
	// Thereafter is the stride at which entries are logged after First; a
	// zero value drops every entry after First.
	Thereafter uint64
}

// SamplerSnapshot bundles a Sampler's configuration snapshot with its
// per-level runtime statistics.
type SamplerSnapshot struct {
	Config SamplerConfigSnapshot
	Stats  []SamplerLevelStats
}

// samplerConfig is the immutable sampling configuration stored behind an
// atomic pointer, so that parameter updates are published atomically:
// concurrent Check calls observe either the previous configuration or the
// new one in full, never a mix.
type samplerConfig struct {
	tick       time.Duration
	first      uint64
	thereafter uint64
}

// samplerStats holds the per-level runtime counters. The fixed-size arrays
// of atomics must never be copied, so samplers share a pointer to it.
type samplerStats struct {
	accepted [_numLevels]atomic.Uint64
	dropped  [_numLevels]atomic.Uint64
	resets   [_numLevels]atomic.Uint64
}

// samplerState bundles everything shared between a sampler and the Cores
// derived from it with With: the per-message counters, the runtime
// statistics, and the active configuration. Separate samplers never share
// state, so their statistics can't be mixed by concurrent logging.
type samplerState struct {
	counts *counters
	stats  *samplerStats
	config atomic.Pointer[samplerConfig]
}

func newSamplerState(tick time.Duration, first, thereafter int) *samplerState {
	st := &samplerState{
		counts: newCounters(),
		stats:  &samplerStats{},
	}
	st.config.Store(&samplerConfig{
		tick:       tick,
		first:      uint64(first),
		thereafter: uint64(thereafter),
	})
	return st
}

// optionFunc wraps a func so it satisfies the SamplerOption interface.
type optionFunc func(*sampler)

func (f optionFunc) apply(s *sampler) {
	f(s)
}

// SamplerOption configures a Sampler.
type SamplerOption interface {
	apply(*sampler)
}

// nopSamplingHook is the default hook used by sampler.
func nopSamplingHook(Entry, SamplingDecision) {}

// SamplerHook registers a function  which will be called when Sampler makes a
// decision.
//
// This hook may be used to get visibility into the performance of the sampler.
// For example, use it to track metrics of dropped versus sampled logs.
//
//	var dropped atomic.Int64
//	zapcore.SamplerHook(func(ent zapcore.Entry, dec zapcore.SamplingDecision) {
//	  if dec&zapcore.LogDropped > 0 {
//	    dropped.Inc()
//	  }
//	})
func SamplerHook(hook func(entry Entry, dec SamplingDecision)) SamplerOption {
	return optionFunc(func(s *sampler) {
		s.hook = hook
	})
}

// NewSamplerWithOptions creates a Core that samples incoming entries, which
// caps the CPU and I/O load of logging while attempting to preserve a
// representative subset of your logs.
//
// Zap samples by logging the first N entries with a given level and message
// each tick. If more Entries with the same level and message are seen during
// the same interval, every Mth message is logged and the rest are dropped.
//
// For example,
//
//	core = NewSamplerWithOptions(core, time.Second, 10, 5)
//
// This will log the first 10 log entries with the same level and message
// in a one second interval as-is. Following that, it will allow through
// every 5th log entry with the same level and message in that interval.
//
// If thereafter is zero, the Core will drop all log entries after the first N
// in that interval.
//
// Sampler can be configured to report sampling decisions with the SamplerHook
// option.
//
// Beyond SamplerHook, the returned Core implements [SamplerReporter], which
// exposes per-level runtime counters (accepted entries, dropped entries, and
// sampling-window resets), a configuration snapshot, and atomic runtime
// reconfiguration via UpdateSamplerConfig.
//
// Keep in mind that Zap's sampling implementation is optimized for speed over
// absolute precision; under load, each tick may be slightly over- or
// under-sampled.
func NewSamplerWithOptions(core Core, tick time.Duration, first, thereafter int, opts ...SamplerOption) Core {
	s := &sampler{
		Core:  core,
		state: newSamplerState(tick, first, thereafter),
		hook:  nopSamplingHook,
	}
	for _, opt := range opts {
		opt.apply(s)
	}

	return s
}

type sampler struct {
	Core

	state *samplerState
	hook  func(Entry, SamplingDecision)
}

var (
	_ Core            = (*sampler)(nil)
	_ leveledEnabler  = (*sampler)(nil)
	_ SamplerReporter = (*sampler)(nil)
)

// NewSampler creates a Core that samples incoming entries, which
// caps the CPU and I/O load of logging while attempting to preserve a
// representative subset of your logs.
//
// Zap samples by logging the first N entries with a given level and message
// each tick. If more Entries with the same level and message are seen during
// the same interval, every Mth message is logged and the rest are dropped.
//
// Keep in mind that zap's sampling implementation is optimized for speed over
// absolute precision; under load, each tick may be slightly over- or
// under-sampled.
//
// Deprecated: use NewSamplerWithOptions.
func NewSampler(core Core, tick time.Duration, first, thereafter int) Core {
	return NewSamplerWithOptions(core, tick, first, thereafter)
}

func (s *sampler) Level() Level {
	return LevelOf(s.Core)
}

func (s *sampler) With(fields []Field) Core {
	return &sampler{
		Core:  s.Core.With(fields),
		state: s.state,
		hook:  s.hook,
	}
}

// SamplerStats returns a point-in-time snapshot of the sampler's runtime
// counters, ordered from DebugLevel to FatalLevel. Cores derived with With
// report the same counters as the sampler they came from.
func (s *sampler) SamplerStats() []SamplerLevelStats {
	stats := make([]SamplerLevelStats, _numLevels)
	for i := 0; i < int(_numLevels); i++ {
		lvl := Level(int(_minLevel) + i)
		idx := lvl - _minLevel
		stats[i] = SamplerLevelStats{
			Level:    lvl,
			Accepted: s.state.stats.accepted[idx].Load(),
			Dropped:  s.state.stats.dropped[idx].Load(),
			Resets:   s.state.stats.resets[idx].Load(),
		}
	}
	return stats
}

// SamplerConfig returns a point-in-time snapshot of the sampler's active
// configuration.
func (s *sampler) SamplerConfig() SamplerConfigSnapshot {
	cfg := s.state.config.Load()
	return SamplerConfigSnapshot{
		Tick:       cfg.tick,
		First:      cfg.first,
		Thereafter: cfg.thereafter,
	}
}

// SamplerSnapshot returns the sampler's active configuration together with
// its per-level runtime counters. Each value is read atomically.
func (s *sampler) SamplerSnapshot() SamplerSnapshot {
	return SamplerSnapshot{
		Config: s.SamplerConfig(),
		Stats:  s.SamplerStats(),
	}
}

// UpdateSamplerConfig atomically replaces the sampler's sampling parameters:
// tick is the sampling window, first is the per-window burst, and thereafter
// is the steady-state stride (zero drops everything after the burst).
//
// Concurrent Check calls observe either the previous configuration or the
// new one in full, never a torn mix. The update applies to every Core
// derived from this sampler with With.
func (s *sampler) UpdateSamplerConfig(tick time.Duration, first, thereafter int) {
	s.state.config.Store(&samplerConfig{
		tick:       tick,
		first:      uint64(first),
		thereafter: uint64(thereafter),
	})
}

func (s *sampler) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if !s.Enabled(ent.Level) {
		return ce
	}

	if ent.Level >= _minLevel && ent.Level <= _maxLevel {
		cfg := s.state.config.Load()
		counter := s.state.counts.get(ent.Level, ent.Message)
		n, reset := counter.IncCheckReset(ent.Time, cfg.tick)
		idx := ent.Level - _minLevel
		if reset {
			s.state.stats.resets[idx].Add(1)
		}
		if n > cfg.first && (cfg.thereafter == 0 || (n-cfg.first)%cfg.thereafter != 0) {
			s.state.stats.dropped[idx].Add(1)
			s.hook(ent, LogDropped)
			return ce
		}
		s.state.stats.accepted[idx].Add(1)
		s.hook(ent, LogSampled)
	}
	return s.Core.Check(ent, ce)
}
