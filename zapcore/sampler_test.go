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

package zapcore_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/internal/ztest"
	//revive:disable:dot-imports
	. "go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeSampler(lvl LevelEnabler, tick time.Duration, first, thereafter int) (Core, *observer.ObservedLogs) {
	core, logs := observer.New(lvl)
	// Keep using deprecated constructor for cc.
	core = NewSampler(core, tick, first, thereafter)
	return core, logs
}

func assertSequence(t testing.TB, logs []observer.LoggedEntry, lvl Level, seq ...int64) {
	seen := make([]int64, len(logs))
	for i, entry := range logs {
		require.Equal(t, "", entry.Message, "Message wasn't created by writeSequence.")
		require.Equal(t, 1, len(entry.Context), "Unexpected number of fields.")
		require.Equal(t, lvl, entry.Level, "Unexpected level.")
		f := entry.Context[0]
		require.Equal(t, "iter", f.Key, "Unexpected field key.")
		require.Equal(t, Int64Type, f.Type, "Unexpected field type")
		seen[i] = f.Integer
	}
	assert.Equal(t, seq, seen, "Unexpected sequence logged at level %v.", lvl)
}

func writeSequence(core Core, n int, lvl Level) {
	// All tests using writeSequence verify that counters are shared between
	// parent and child cores.
	core = core.With([]Field{makeInt64Field("iter", n)})
	if ce := core.Check(Entry{Level: lvl, Time: time.Now()}, nil); ce != nil {
		ce.Write()
	}
}

func TestSampler(t *testing.T) {
	for _, lvl := range []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, DPanicLevel, PanicLevel, FatalLevel} {
		sampler, logs := fakeSampler(DebugLevel, time.Minute, 2, 3)

		// Ensure that counts aren't shared between levels.
		probeLevel := DebugLevel
		if lvl == DebugLevel {
			probeLevel = InfoLevel
		}
		for i := 0; i < 10; i++ {
			writeSequence(sampler, 1, probeLevel)
		}
		// Clear any output.
		logs.TakeAll()

		for i := 1; i < 10; i++ {
			writeSequence(sampler, i, lvl)
		}
		assertSequence(t, logs.TakeAll(), lvl, 1, 2, 5, 8)
	}
}

func TestLevelOfSampler(t *testing.T) {
	levels := []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, DPanicLevel, PanicLevel, FatalLevel}
	for _, lvl := range levels {
		lvl := lvl
		t.Run(lvl.String(), func(t *testing.T) {
			t.Parallel()

			sampler, _ := fakeSampler(lvl, time.Minute, 2, 3)
			assert.Equal(t, lvl, LevelOf(sampler), "Sampler level did not match.")
		})
	}
}

func TestSamplerDisabledLevels(t *testing.T) {
	sampler, logs := fakeSampler(InfoLevel, time.Minute, 1, 100)

	// Shouldn't be counted, because debug logging isn't enabled.
	writeSequence(sampler, 1, DebugLevel)
	writeSequence(sampler, 2, InfoLevel)
	assertSequence(t, logs.TakeAll(), InfoLevel, 2)
}

func TestSamplerTicking(t *testing.T) {
	// Ensure that we're resetting the sampler's counter every tick.
	sampler, logs := fakeSampler(DebugLevel, 10*time.Millisecond, 5, 10)

	// If we log five or fewer messages every tick, none of them should be
	// dropped.
	for tick := 0; tick < 2; tick++ {
		for i := 1; i <= 5; i++ {
			writeSequence(sampler, i, InfoLevel)
		}
		ztest.Sleep(15 * time.Millisecond)
	}
	assertSequence(
		t,
		logs.TakeAll(),
		InfoLevel,
		1, 2, 3, 4, 5, // first tick
		1, 2, 3, 4, 5, // second tick
	)

	// If we log quickly, we should drop some logs. The first five statements
	// each tick should be logged, then every tenth.
	for tick := 0; tick < 3; tick++ {
		for i := 1; i < 18; i++ {
			writeSequence(sampler, i, InfoLevel)
		}
		ztest.Sleep(10 * time.Millisecond)
	}

	assertSequence(
		t,
		logs.TakeAll(),
		InfoLevel,
		1, 2, 3, 4, 5, 15, // first tick
		1, 2, 3, 4, 5, 15, // second tick
		1, 2, 3, 4, 5, 15, // third tick
	)
}

type countingCore struct {
	logs atomic.Uint32
}

func (c *countingCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	return ce.AddCore(ent, c)
}

func (c *countingCore) Write(Entry, []Field) error {
	c.logs.Add(1)
	return nil
}

func (c *countingCore) With([]Field) Core { return c }
func (*countingCore) Enabled(Level) bool  { return true }
func (*countingCore) Sync() error         { return nil }

func TestSamplerConcurrent(t *testing.T) {
	const (
		logsPerTick   = 10
		numMessages   = 5
		numTicks      = 25
		numGoroutines = 10
		tick          = 10 * time.Millisecond

		// We'll make a total of,
		// (numGoroutines * numTicks * logsPerTick * 2) log attempts
		// with numMessages unique messages.
		numLogAttempts = numGoroutines * logsPerTick * numTicks * 2
		// Of those, we'll accept (logsPerTick * numTicks) entries
		// for each unique message.
		expectedCount = numMessages * logsPerTick * numTicks
		// The rest will be dropped.
		expectedDropped = numLogAttempts - expectedCount
	)

	clock := ztest.NewMockClock()

	cc := &countingCore{}

	hook, dropped, sampled := makeSamplerCountingHook()
	sampler := NewSamplerWithOptions(cc, tick, logsPerTick, 100000, SamplerHook(hook))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int, ticker *time.Ticker) {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-stop:
					return

				case <-ticker.C:
					for j := 0; j < logsPerTick*2; j++ {
						msg := fmt.Sprintf("msg%v", i%numMessages)
						ent := Entry{
							Level:   DebugLevel,
							Message: msg,
							Time:    clock.Now(),
						}
						if ce := sampler.Check(ent, nil); ce != nil {
							ce.Write()
						}

						// Give a chance for other goroutines to run.
						runtime.Gosched()
					}
				}
			}
		}(i, clock.NewTicker(tick))
	}

	clock.Add(tick * numTicks)
	close(stop)
	wg.Wait()

	assert.Equal(
		t,
		expectedCount,
		int(cc.logs.Load()),
		"Unexpected number of logs",
	)
	assert.Equal(t,
		expectedCount,
		int(sampled.Load()),
		"Unexpected number of logs sampled",
	)
	assert.Equal(t,
		expectedDropped,
		int(dropped.Load()),
		"Unexpected number of logs dropped",
	)
}

func TestSamplerRaces(t *testing.T) {
	sampler, _ := fakeSampler(DebugLevel, time.Minute, 1, 1000)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			<-start
			for j := 0; j < 100; j++ {
				writeSequence(sampler, j, InfoLevel)
			}
			wg.Done()
		}()
	}

	close(start)
	wg.Wait()
}

func TestSamplerUnknownLevels(t *testing.T) {
	// Prove that out-of-bounds levels don't panic.
	unknownLevels := []Level{
		DebugLevel - 1,
		FatalLevel + 1,
	}

	for _, lvl := range unknownLevels {
		t.Run(lvl.String(), func(t *testing.T) {
			sampler, logs := fakeSampler(lvl, time.Minute, 2, 3)
			for i := 1; i < 10; i++ {
				writeSequence(sampler, i, lvl)
			}

			// Expect no sampling for unknown levels.
			assertSequence(t, logs.TakeAll(), lvl, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		})
	}
}

func TestSamplerWithZeroThereafter(t *testing.T) {
	var counter countingCore

	// Logs two messages per second.
	sampler := NewSamplerWithOptions(&counter, time.Second, 2, 0)

	now := time.Now()

	for i := 0; i < 1000; i++ {
		ent := Entry{
			Level:   InfoLevel,
			Message: "msg",
			Time:    now,
		}
		if ce := sampler.Check(ent, nil); ce != nil {
			ce.Write()
		}
	}

	assert.Equal(t, 2, int(counter.logs.Load()),
		"Unexpected number of logs")

	now = now.Add(time.Second)

	for i := 0; i < 1000; i++ {
		ent := Entry{
			Level:   InfoLevel,
			Message: "msg",
			Time:    now,
		}
		if ce := sampler.Check(ent, nil); ce != nil {
			ce.Write()
		}
	}

	assert.Equal(t, 4, int(counter.logs.Load()),
		"Unexpected number of logs")
}

// writeSamplerEntry checks and writes a single entry with a fixed message and
// timestamp directly through the sampler.
func writeSamplerEntry(core Core, lvl Level, msg string, at time.Time) {
	if ce := core.Check(Entry{Level: lvl, Message: msg, Time: at}, nil); ce != nil {
		ce.Write()
	}
}

func levelSamplerStats(t *testing.T, stats []SamplerLevelStats, lvl Level) SamplerLevelStats {
	t.Helper()
	for _, s := range stats {
		if s.Level == lvl {
			return s
		}
	}
	t.Fatalf("level %v not found in sampler stats", lvl)
	return SamplerLevelStats{}
}

func assertLevelSamplerStats(
	t *testing.T,
	reporter SamplerReporter,
	lvl Level,
	accepted, dropped, resets uint64,
	msgAndArgs ...interface{},
) {
	t.Helper()
	got := levelSamplerStats(t, reporter.SamplerStats(), lvl)
	assert.Equal(t, SamplerLevelStats{
		Level:    lvl,
		Accepted: accepted,
		Dropped:  dropped,
		Resets:   resets,
	}, got, msgAndArgs...)
}

func TestSamplerRuntimeStats(t *testing.T) {
	// Per-level accepted/dropped counters must be tracked independently.
	core, logs := observer.New(DebugLevel)
	samplerCore := NewSamplerWithOptions(core, time.Minute, 2, 0)
	reporter := samplerCore.(SamplerReporter)

	now := time.Now()

	// 5 info attempts within the same window: first 2 accepted, 3 dropped.
	for i := 0; i < 5; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "info-msg", now)
	}
	// 4 warn attempts: 2 accepted, 2 dropped.
	for i := 0; i < 4; i++ {
		writeSamplerEntry(samplerCore, WarnLevel, "warn-msg", now)
	}

	assertLevelSamplerStats(t, reporter, InfoLevel, 2, 3, 1)
	assertLevelSamplerStats(t, reporter, WarnLevel, 2, 2, 1)

	// Levels that saw no traffic must stay at zero.
	for _, lvl := range []Level{DebugLevel, ErrorLevel, DPanicLevel, PanicLevel, FatalLevel} {
		assertLevelSamplerStats(t, reporter, lvl, 0, 0, 0)
	}

	assert.Len(t, logs.FilterLevelExact(InfoLevel).TakeAll(), 2,
		"unexpected number of info logs written")
	assert.Len(t, logs.FilterLevelExact(WarnLevel).TakeAll(), 2,
		"unexpected number of warn logs written")
}

func TestSamplerBurstWindowStats(t *testing.T) {
	// The initial burst resets every tick; the reset counter must report
	// one transition per window per level/message.
	core, logs := observer.New(DebugLevel)
	samplerCore := NewSamplerWithOptions(core, time.Minute, 3, 0)
	reporter := samplerCore.(SamplerReporter)

	t0 := time.Unix(1700000000, 0)

	// First window: burst of 3, then 7 drops.
	for i := 0; i < 10; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "burst", t0)
	}
	assertLevelSamplerStats(t, reporter, InfoLevel, 3, 7, 1)

	// Second window: counters reset, another burst of 3.
	t1 := t0.Add(time.Minute)
	for i := 0; i < 10; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "burst", t1)
	}
	assertLevelSamplerStats(t, reporter, InfoLevel, 6, 14, 2)

	// Third window with fewer entries than the burst accepts everything.
	t2 := t0.Add(2 * time.Minute)
	for i := 0; i < 2; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "burst", t2)
	}
	assertLevelSamplerStats(t, reporter, InfoLevel, 8, 14, 3)
	assert.Len(t, logs.TakeAll(), 8, "unexpected number of logs written")
}

func TestSamplerConfigSnapshot(t *testing.T) {
	core, _ := observer.New(DebugLevel)
	samplerCore := NewSamplerWithOptions(core, 10*time.Millisecond, 5, 2)
	reporter := samplerCore.(SamplerReporter)

	assert.Equal(t, SamplerConfigSnapshot{
		Tick:       10 * time.Millisecond,
		First:      5,
		Thereafter: 2,
	}, reporter.SamplerConfig(), "unexpected initial config snapshot")

	snap := reporter.SamplerSnapshot()
	assert.Equal(t, reporter.SamplerConfig(), snap.Config,
		"combined snapshot config mismatch")
	assert.Len(t, snap.Stats, len([]Level{
		DebugLevel, InfoLevel, WarnLevel, ErrorLevel,
		DPanicLevel, PanicLevel, FatalLevel,
	}), "snapshot must carry stats for every level")
}

func TestSamplerUpdateSamplerConfig(t *testing.T) {
	core, logs := observer.New(DebugLevel)
	samplerCore := NewSamplerWithOptions(core, time.Hour, 2, 0)
	reporter := samplerCore.(SamplerReporter)

	t0 := time.Unix(1700000000, 0)

	// Initial config: burst of 2 per hour, drop everything afterwards.
	for i := 0; i < 5; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "msg", t0)
	}
	assert.Len(t, logs.TakeAll(), 2)

	// Widen the burst to 5; the next window accepts 5 of 10.
	reporter.UpdateSamplerConfig(time.Hour, 5, 0)
	assert.Equal(t, SamplerConfigSnapshot{
		Tick: time.Hour, First: 5, Thereafter: 0,
	}, reporter.SamplerConfig())

	t1 := t0.Add(time.Hour)
	for i := 0; i < 10; i++ {
		writeSamplerEntry(samplerCore, InfoLevel, "msg", t1)
	}
	assert.Len(t, logs.TakeAll(), 5)

	// Switch to first=1, thereafter=2 in a third window:
	// positions 1, 3, 5, 7, 9 are accepted.
	reporter.UpdateSamplerConfig(time.Hour, 1, 2)
	assert.Equal(t, SamplerConfigSnapshot{
		Tick: time.Hour, First: 1, Thereafter: 2,
	}, reporter.SamplerConfig())

	t2 := t0.Add(2 * time.Hour)
	// With first=1, thereafter=2, positions 1, 3, 5, 7, 9 are accepted.
	accepted := make([]bool, 10)
	for i := range accepted {
		ce := samplerCore.Check(Entry{Level: InfoLevel, Message: "msg", Time: t2}, nil)
		accepted[i] = ce != nil
		if ce != nil {
			ce.Write()
		}
	}
	assert.Equal(t,
		[]bool{true, false, true, false, true, false, true, false, true, false},
		accepted, "unexpected first/thereafter acceptance pattern")
	assert.Len(t, logs.TakeAll(), 5)

	// Three windows -> three resets; 2 + 5 + 5 accepted, 3 + 5 + 5 dropped.
	assertLevelSamplerStats(t, reporter, InfoLevel, 12, 13, 3)
}

func TestSamplerUpdateSamplerConfigConcurrent(t *testing.T) {
	var cc countingCore
	samplerCore := NewSamplerWithOptions(&cc, time.Hour, 1, 1)
	reporter := samplerCore.(SamplerReporter)

	configs := []SamplerConfigSnapshot{
		{Tick: time.Hour, First: 1, Thereafter: 1}, // initial configuration
		{Tick: time.Second, First: 1, Thereafter: 1},
		{Tick: time.Minute, First: 2, Thereafter: 3},
		{Tick: time.Hour, First: 10, Thereafter: 100},
	}
	valid := make(map[SamplerConfigSnapshot]struct{}, len(configs))
	for _, cfg := range configs {
		valid[cfg] = struct{}{}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Configurations are always published as a complete set: a concurrent
	// reader must never observe a torn mix of parameters.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					reporter.UpdateSamplerConfig(
						configs[i%len(configs)].Tick,
						int(configs[i%len(configs)].First),
						int(configs[i%len(configs)].Thereafter),
					)
				}
			}
		}(i)
	}

	// Concurrent readers and loggers must never see an invalid snapshot or
	// race on the configuration (run with -race).
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					cfg := reporter.SamplerConfig()
					assert.Contains(t, valid, cfg,
						"observed torn sampler config: %+v", cfg)
					writeSamplerEntry(samplerCore, DebugLevel, "race", time.Now())
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// The loop exits only if every observed configuration was one of the
	// published sets; fail explicitly if assertions ran on goroutines and
	// could not stop the test.
	require.Contains(t, valid, reporter.SamplerConfig())
}

func TestSamplerStatsSeparateSamplers(t *testing.T) {
	// Independent samplers must never share counters or statistics.
	coreA, logsA := observer.New(DebugLevel)
	samplerA := NewSamplerWithOptions(coreA, time.Minute, 1, 0)
	reporterA := samplerA.(SamplerReporter)

	coreB, logsB := observer.New(DebugLevel)
	samplerB := NewSamplerWithOptions(coreB, time.Minute, 1, 0)
	reporterB := samplerB.(SamplerReporter)

	now := time.Now()

	for i := 0; i < 3; i++ {
		writeSamplerEntry(samplerA, InfoLevel, "msg", now)
	}
	assertLevelSamplerStats(t, reporterA, InfoLevel, 1, 2, 1)
	assertLevelSamplerStats(t, reporterB, InfoLevel, 0, 0, 0)

	for i := 0; i < 2; i++ {
		writeSamplerEntry(samplerB, InfoLevel, "msg", now)
	}
	assertLevelSamplerStats(t, reporterB, InfoLevel, 1, 1, 1)
	assertLevelSamplerStats(t, reporterA, InfoLevel, 1, 2, 1,
		"logging to samplerB must not touch samplerA's counters")

	// A Core derived with With shares counters, stats, and config with its
	// parent, and a config update applies to both.
	child := samplerA.With([]Field{makeInt64Field("k", 1)})
	childReporter := child.(SamplerReporter)
	// In a fresh window, the child's first entry is accepted and the rest
	// dropped, all attributed to the shared counters.
	nextWindow := now.Add(time.Minute)
	for i := 0; i < 3; i++ {
		writeSamplerEntry(child, InfoLevel, "msg", nextWindow)
	}
	assertLevelSamplerStats(t, reporterA, InfoLevel, 2, 4, 2)
	assertLevelSamplerStats(t, childReporter, InfoLevel, 2, 4, 2,
		"child core must report the parent sampler's stats")

	reporterA.UpdateSamplerConfig(time.Hour, 42, 7)
	assert.Equal(t, reporterA.SamplerConfig(), childReporter.SamplerConfig(),
		"config update must apply to With-derived cores")

	assert.Len(t, logsA.TakeAll(), 2)
	assert.Len(t, logsB.TakeAll(), 1)
}

func TestSamplerDPanicLevel(t *testing.T) {
	// DPanic is a regular sampling level at the zapcore layer: accepted and
	// dropped entries are counted per level without panicking.
	core, logs := observer.New(DebugLevel)
	samplerCore := NewSamplerWithOptions(core, time.Minute, 2, 0)
	reporter := samplerCore.(SamplerReporter)

	now := time.Now()
	assert.NotPanics(t, func() {
		for i := 0; i < 4; i++ {
			writeSamplerEntry(samplerCore, DPanicLevel, "dpanic", now)
		}
	})

	assertLevelSamplerStats(t, reporter, DPanicLevel, 2, 2, 1)
	written := logs.TakeAll()
	assert.Len(t, written, 2)
	for _, ent := range written {
		assert.Equal(t, DPanicLevel, ent.Level)
	}
}
