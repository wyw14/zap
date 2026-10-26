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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/internal/exit"
	"go.uber.org/zap/internal/ztest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeCountingHook() (func(zapcore.Entry) error, *atomic.Int64) {
	count := &atomic.Int64{}
	h := func(zapcore.Entry) error {
		count.Add(1)
		return nil
	}
	return h, count
}

func TestLoggerAtomicLevel(t *testing.T) {
	// Test that the dynamic level applies to all ancestors and descendants.
	dl := NewAtomicLevel()

	withLogger(t, dl, nil, func(grandparent *Logger, _ *observer.ObservedLogs) {
		parent := grandparent.With(Int("generation", 1))
		child := parent.With(Int("generation", 2))

		tests := []struct {
			setLevel  zapcore.Level
			testLevel zapcore.Level
			enabled   bool
		}{
			{DebugLevel, DebugLevel, true},
			{InfoLevel, DebugLevel, false},
			{WarnLevel, PanicLevel, true},
		}

		for _, tt := range tests {
			dl.SetLevel(tt.setLevel)
			for _, logger := range []*Logger{grandparent, parent, child} {
				if tt.enabled {
					assert.NotNil(
						t,
						logger.Check(tt.testLevel, ""),
						"Expected level %s to be enabled after setting level %s.", tt.testLevel, tt.setLevel,
					)
				} else {
					assert.Nil(
						t,
						logger.Check(tt.testLevel, ""),
						"Expected level %s to be enabled after setting level %s.", tt.testLevel, tt.setLevel,
					)
				}
			}
		}
	})
}

func TestLoggerLevel(t *testing.T) {
	levels := []zapcore.Level{
		DebugLevel,
		InfoLevel,
		WarnLevel,
		ErrorLevel,
		DPanicLevel,
		PanicLevel,
		FatalLevel,
	}

	for _, lvl := range levels {
		lvl := lvl
		t.Run(lvl.String(), func(t *testing.T) {
			t.Parallel()

			core, _ := observer.New(lvl)
			log := New(core)
			assert.Equal(t, lvl, log.Level())
		})
	}

	t.Run("Nop", func(t *testing.T) {
		assert.Equal(t, zapcore.InvalidLevel, NewNop().Level())
	})
}

func TestLoggerInitialFields(t *testing.T) {
	fieldOpts := opts(Fields(Int("foo", 42), String("bar", "baz")))
	withLogger(t, DebugLevel, fieldOpts, func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Info("")
		assert.Equal(
			t,
			observer.LoggedEntry{Context: []Field{Int("foo", 42), String("bar", "baz")}},
			logs.AllUntimed()[0],
			"Unexpected output with initial fields set.",
		)
	})
}

func TestLoggerWith(t *testing.T) {
	tests := []struct {
		name          string
		initialFields []Field
		withMethod    func(*Logger, ...Field) *Logger
	}{
		{
			"regular non lazy logger",
			[]Field{Int("foo", 42)},
			(*Logger).With,
		},
		{
			"regular non lazy logger no initial fields",
			[]Field{},
			(*Logger).With,
		},
		{
			"lazy with logger",
			[]Field{Int("foo", 42)},
			(*Logger).WithLazy,
		},
		{
			"lazy with logger no initial fields",
			[]Field{},
			(*Logger).WithLazy,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withLogger(t, DebugLevel, opts(Fields(tt.initialFields...)), func(logger *Logger, logs *observer.ObservedLogs) {
				// Child loggers should have copy-on-write semantics, so two children
				// shouldn't stomp on each other's fields or affect the parent's fields.
				tt.withMethod(logger).Info("")
				tt.withMethod(logger, String("one", "two")).Info("")
				tt.withMethod(logger, String("three", "four")).Info("")
				tt.withMethod(logger, String("five", "six")).With(String("seven", "eight")).Info("")
				logger.Info("")

				assert.Equal(t, []observer.LoggedEntry{
					{Context: tt.initialFields},
					{Context: append(tt.initialFields, String("one", "two"))},
					{Context: append(tt.initialFields, String("three", "four"))},
					{Context: append(tt.initialFields, String("five", "six"), String("seven", "eight"))},
					{Context: tt.initialFields},
				}, logs.AllUntimed(), "Unexpected cross-talk between child loggers.")
			})
		})
	}
}

func TestLoggerWithCaptures(t *testing.T) {
	type withF func(*Logger, ...Field) *Logger
	tests := []struct {
		name        string
		withMethods []withF
		wantJSON    []string
	}{
		{
			name:        "regular with captures arguments at time of With",
			withMethods: []withF{(*Logger).With},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [0],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [0],
					"c0": [2]
				}`,
			},
		},
		{
			name:        "lazy with captures arguments at time of With or Logging",
			withMethods: []withF{(*Logger).WithLazy},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [1],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [1],
					"c0": [2]
				}`,
			},
		},
		{
			name:        "2x With captures arguments at time of each With",
			withMethods: []withF{(*Logger).With, (*Logger).With},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [0],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [0],
					"c0": [2]
				}`,
				`{
					"m": "hello 1",
					"a0": [0],
					"c0": [2],
					"a1": [10],
					"b1": [11]
				}`,
				`{
					"m": "world 1",
					"a0": [0],
					"c0": [2],
					"a1": [10],
					"c1": [12]
				}`,
			},
		},
		{
			name:        "2x WithLazy. Captures arguments only at logging time.",
			withMethods: []withF{(*Logger).WithLazy, (*Logger).WithLazy},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [1],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [1],
					"c0": [2]
				}`,
				`{
					"m": "hello 1",
					"a0": [1],
					"c0": [2],
					"a1": [11],
					"b1": [11]
				}`,
				`{
					"m": "world 1",
					"a0": [1],
					"c0": [2],
					"a1": [11],
					"c1": [12]
				}`,
			},
		},
		{
			name:        "WithLazy then With",
			withMethods: []withF{(*Logger).WithLazy, (*Logger).With},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [1],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [1],
					"c0": [2]
				}`,
				`{
					"m": "hello 1",
					"a0": [1],
					"c0": [2],
					"a1": [10],
					"b1": [11]
				}`,
				`{
					"m": "world 1",
					"a0": [1],
					"c0": [2],
					"a1": [10],
					"c1": [12]
				}`,
			},
		},
		{
			name:        "With then WithLazy",
			withMethods: []withF{(*Logger).With, (*Logger).WithLazy},
			wantJSON: []string{
				`{
					"m": "hello 0",
					"a0": [0],
					"b0": [1]
				}`,
				`{
					"m": "world 0",
					"a0": [0],
					"c0": [2]
				}`,
				`{
					"m": "hello 1",
					"a0": [0],
					"c0": [2],
					"a1": [11],
					"b1": [11]
				}`,
				`{
					"m": "world 1",
					"a0": [0],
					"c0": [2],
					"a1": [11],
					"c1": [12]
				}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
				MessageKey: "m",
			})

			var bs ztest.Buffer
			logger := New(zapcore.NewCore(enc, &bs, DebugLevel))

			for i, withMethod := range tt.withMethods {

				iStr := strconv.Itoa(i)
				x := 10 * i
				arr := zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
					enc.AppendInt(x)
					return nil
				})

				// Demonstrate the arguments are captured when With() and Info() are invoked.
				logger = withMethod(logger, Array("a"+iStr, arr))
				x++
				logger.Info(fmt.Sprintf("hello %d", i), Array("b"+iStr, arr))
				x++
				logger = withMethod(logger, Array("c"+iStr, arr))
				logger.Info(fmt.Sprintf("world %d", i))
			}

			if lines := bs.Lines(); assert.Len(t, lines, len(tt.wantJSON)) {
				for i, want := range tt.wantJSON {
					assert.JSONEq(t, want, lines[i], "Unexpected output from the %d'th log.", i)
				}
			}
		})
	}
}

func TestLoggerLogPanic(t *testing.T) {
	for _, tt := range []struct {
		do       func(*Logger)
		should   bool
		expected string
	}{
		{func(logger *Logger) { logger.Check(PanicLevel, "foo").Write() }, true, "foo"},
		{func(logger *Logger) { logger.Log(PanicLevel, "bar") }, true, "bar"},
		{func(logger *Logger) { logger.Panic("baz") }, true, "baz"},
	} {
		withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
			if tt.should {
				assert.Panics(t, func() { tt.do(logger) }, "Expected panic")
			} else {
				assert.NotPanics(t, func() { tt.do(logger) }, "Expected no panic")
			}

			output := logs.AllUntimed()
			assert.Equal(t, 1, len(output), "Unexpected number of logs.")
			assert.Equal(t, 0, len(output[0].Context), "Unexpected context on first log.")
			assert.Equal(
				t,
				zapcore.Entry{Message: tt.expected, Level: PanicLevel},
				output[0].Entry,
				"Unexpected output from panic-level Log.",
			)
		})
	}
}

func TestLoggerLogFatal(t *testing.T) {
	for _, tt := range []struct {
		do       func(*Logger)
		expected string
	}{
		{func(logger *Logger) { logger.Check(FatalLevel, "foo").Write() }, "foo"},
		{func(logger *Logger) { logger.Log(FatalLevel, "bar") }, "bar"},
		{func(logger *Logger) { logger.Fatal("baz") }, "baz"},
	} {
		withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
			stub := exit.WithStub(func() {
				tt.do(logger)
			})
			assert.True(t, stub.Exited, "Expected Fatal logger call to terminate process.")
			output := logs.AllUntimed()
			assert.Equal(t, 1, len(output), "Unexpected number of logs.")
			assert.Equal(t, 0, len(output[0].Context), "Unexpected context on first log.")
			assert.Equal(
				t,
				zapcore.Entry{Message: tt.expected, Level: FatalLevel},
				output[0].Entry,
				"Unexpected output from fatal-level Log.",
			)
		})
	}
}

func TestLoggerLeveledMethods(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		tests := []struct {
			method        func(string, ...Field)
			expectedLevel zapcore.Level
		}{
			{logger.Debug, DebugLevel},
			{logger.Info, InfoLevel},
			{logger.Warn, WarnLevel},
			{logger.Error, ErrorLevel},
			{logger.DPanic, DPanicLevel},
		}
		for i, tt := range tests {
			tt.method("")
			output := logs.AllUntimed()
			assert.Equal(t, i+1, len(output), "Unexpected number of logs.")
			assert.Equal(t, 0, len(output[i].Context), "Unexpected context on first log.")
			assert.Equal(
				t,
				zapcore.Entry{Level: tt.expectedLevel},
				output[i].Entry,
				"Unexpected output from %s-level logger method.", tt.expectedLevel)
		}
	})
}

func TestLoggerLogLevels(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		levels := []zapcore.Level{
			DebugLevel,
			InfoLevel,
			WarnLevel,
			ErrorLevel,
			DPanicLevel,
		}
		for i, level := range levels {
			logger.Log(level, "")
			output := logs.AllUntimed()
			assert.Equal(t, i+1, len(output), "Unexpected number of logs.")
			assert.Equal(t, 0, len(output[i].Context), "Unexpected context on first log.")
			assert.Equal(
				t,
				zapcore.Entry{Level: level},
				output[i].Entry,
				"Unexpected output from %s-level logger method.", level)
		}
	})
}

func TestLoggerAlwaysPanics(t *testing.T) {
	// Users can disable writing out panic-level logs, but calls to logger.Panic()
	// should still call panic().
	withLogger(t, FatalLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		msg := "Even if output is disabled, logger.Panic should always panic."
		assert.Panics(t, func() { logger.Panic("foo") }, msg)
		assert.Panics(t, func() { logger.Log(PanicLevel, "foo") }, msg)
		assert.Panics(t, func() {
			if ce := logger.Check(PanicLevel, "foo"); ce != nil {
				ce.Write()
			}
		}, msg)
		assert.Equal(t, 0, logs.Len(), "Panics shouldn't be written out if PanicLevel is disabled.")
	})
}

func TestLoggerAlwaysFatals(t *testing.T) {
	// Users can disable writing out fatal-level logs, but calls to logger.Fatal()
	// should still terminate the process.
	withLogger(t, FatalLevel+1, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		stub := exit.WithStub(func() { logger.Fatal("") })
		assert.True(t, stub.Exited, "Expected calls to logger.Fatal to terminate process.")

		stub = exit.WithStub(func() { logger.Log(FatalLevel, "") })
		assert.True(t, stub.Exited, "Expected calls to logger.Fatal to terminate process.")

		stub = exit.WithStub(func() {
			if ce := logger.Check(FatalLevel, ""); ce != nil {
				ce.Write()
			}
		})
		assert.True(t, stub.Exited, "Expected calls to logger.Check(FatalLevel, ...) to terminate process.")

		assert.Equal(t, 0, logs.Len(), "Shouldn't write out logs when fatal-level logging is disabled.")
	})
}

func TestLoggerDPanic(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		assert.NotPanics(t, func() { logger.DPanic("") })
		assert.Equal(
			t,
			[]observer.LoggedEntry{{Entry: zapcore.Entry{Level: DPanicLevel}, Context: []Field{}}},
			logs.AllUntimed(),
			"Unexpected log output from DPanic in production mode.",
		)
	})
	withLogger(t, DebugLevel, opts(Development()), func(logger *Logger, logs *observer.ObservedLogs) {
		assert.Panics(t, func() { logger.DPanic("") })
		assert.Equal(
			t,
			[]observer.LoggedEntry{{Entry: zapcore.Entry{Level: DPanicLevel}, Context: []Field{}}},
			logs.AllUntimed(),
			"Unexpected log output from DPanic in development mode.",
		)
	})
}

func TestLoggerNoOpsDisabledLevels(t *testing.T) {
	withLogger(t, WarnLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Info("silence!")
		assert.Equal(
			t,
			[]observer.LoggedEntry{},
			logs.AllUntimed(),
			"Expected logging at a disabled level to produce no output.",
		)
	})
}

func TestLoggerNames(t *testing.T) {
	tests := []struct {
		names    []string
		expected string
	}{
		{nil, ""},
		{[]string{""}, ""},
		{[]string{"foo"}, "foo"},
		{[]string{"foo", ""}, "foo"},
		{[]string{"foo", "bar"}, "foo.bar"},
		{[]string{"foo.bar", "baz"}, "foo.bar.baz"},
		// Garbage in, garbage out.
		{[]string{"foo.", "bar"}, "foo..bar"},
		{[]string{"foo", ".bar"}, "foo..bar"},
		{[]string{"foo.", ".bar"}, "foo...bar"},
	}

	for _, tt := range tests {
		withLogger(t, DebugLevel, nil, func(log *Logger, logs *observer.ObservedLogs) {
			for _, n := range tt.names {
				log = log.Named(n)
			}
			log.Info("")
			require.Equal(t, 1, logs.Len(), "Expected only one log entry to be written.")
			assert.Equal(t, tt.expected, logs.AllUntimed()[0].LoggerName, "Unexpected logger name from entry.")
			assert.Equal(t, tt.expected, log.Name(), "Unexpected logger name.")
		})
		withSugar(t, DebugLevel, nil, func(log *SugaredLogger, logs *observer.ObservedLogs) {
			for _, n := range tt.names {
				log = log.Named(n)
			}
			log.Infow("")
			require.Equal(t, 1, logs.Len(), "Expected only one log entry to be written.")
			assert.Equal(t, tt.expected, logs.AllUntimed()[0].LoggerName, "Unexpected logger name from entry.")
			assert.Equal(t, tt.expected, log.base.Name(), "Unexpected logger name.")
		})
	}
}

func TestLoggerWriteFailure(t *testing.T) {
	errSink := &ztest.Buffer{}
	logger := New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(NewProductionConfig().EncoderConfig),
			zapcore.Lock(zapcore.AddSync(ztest.FailWriter{})),
			DebugLevel,
		),
		ErrorOutput(errSink),
	)

	logger.Info("foo")
	// Should log the error.
	assert.Regexp(t, `write error: failed`, errSink.Stripped(), "Expected to log the error to the error output.")
	assert.True(t, errSink.Called(), "Expected logging an internal error to call Sync the error sink.")
}

func TestLoggerSync(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(logger *Logger, _ *observer.ObservedLogs) {
		assert.NoError(t, logger.Sync(), "Expected syncing a test logger to succeed.")
		assert.NoError(t, logger.Sugar().Sync(), "Expected syncing a sugared logger to succeed.")
	})
}

func TestLoggerSyncFail(t *testing.T) {
	noSync := &ztest.Buffer{}
	err := errors.New("fail")
	noSync.SetError(err)
	logger := New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{}),
		noSync,
		DebugLevel,
	))
	assert.Equal(t, err, logger.Sync(), "Expected Logger.Sync to propagate errors.")
	assert.Equal(t, err, logger.Sugar().Sync(), "Expected SugaredLogger.Sync to propagate errors.")
}

func TestLoggerAddCaller(t *testing.T) {
	tests := []struct {
		options []Option
		pat     string
	}{
		{opts(), `^undefined$`},
		{opts(WithCaller(false)), `^undefined$`},
		{opts(AddCaller()), `.+/logger_test.go:[\d]+$`},
		{opts(AddCaller(), WithCaller(false)), `^undefined$`},
		{opts(WithCaller(true)), `.+/logger_test.go:[\d]+$`},
		{opts(WithCaller(true), WithCaller(false)), `^undefined$`},
		{opts(AddCaller(), AddCallerSkip(1), AddCallerSkip(-1)), `.+/logger_test.go:[\d]+$`},
		{opts(AddCaller(), AddCallerSkip(1)), `.+/common_test.go:[\d]+$`},
		{opts(AddCaller(), AddCallerSkip(1), AddCallerSkip(3)), `.+/src/runtime/.*:[\d]+$`},
	}
	for _, tt := range tests {
		withLogger(t, DebugLevel, tt.options, func(logger *Logger, logs *observer.ObservedLogs) {
			// Make sure that sugaring and desugaring resets caller skip properly.
			logger = logger.Sugar().Desugar()
			logger.Info("")
			output := logs.AllUntimed()
			assert.Equal(t, 1, len(output), "Unexpected number of logs written out.")
			assert.Regexp(
				t,
				tt.pat,
				output[0].Caller,
				"Expected to find package name and file name in output.",
			)
		})
	}
}

func TestLoggerAddCallerFunction(t *testing.T) {
	tests := []struct {
		options         []Option
		loggerFunction  string
		sugaredFunction string
	}{
		{
			options:         opts(),
			loggerFunction:  "",
			sugaredFunction: "",
		},
		{
			options:         opts(WithCaller(false)),
			loggerFunction:  "",
			sugaredFunction: "",
		},
		{
			options:         opts(AddCaller()),
			loggerFunction:  "go.uber.org/zap.infoLog",
			sugaredFunction: "go.uber.org/zap.infoLogSugared",
		},
		{
			options:         opts(AddCaller(), WithCaller(false)),
			loggerFunction:  "",
			sugaredFunction: "",
		},
		{
			options:         opts(WithCaller(true)),
			loggerFunction:  "go.uber.org/zap.infoLog",
			sugaredFunction: "go.uber.org/zap.infoLogSugared",
		},
		{
			options:         opts(WithCaller(true), WithCaller(false)),
			loggerFunction:  "",
			sugaredFunction: "",
		},
		{
			options:         opts(AddCaller(), AddCallerSkip(1), AddCallerSkip(-1)),
			loggerFunction:  "go.uber.org/zap.infoLog",
			sugaredFunction: "go.uber.org/zap.infoLogSugared",
		},
		{
			options:         opts(AddCaller(), AddCallerSkip(2)),
			loggerFunction:  "go.uber.org/zap.withLogger",
			sugaredFunction: "go.uber.org/zap.withLogger",
		},
		{
			options:         opts(AddCaller(), AddCallerSkip(2), AddCallerSkip(3)),
			loggerFunction:  "runtime.goexit",
			sugaredFunction: "runtime.goexit",
		},
	}
	for _, tt := range tests {
		withLogger(t, DebugLevel, tt.options, func(logger *Logger, logs *observer.ObservedLogs) {
			// Make sure that sugaring and desugaring resets caller skip properly.
			logger = logger.Sugar().Desugar()
			infoLog(logger, "")
			infoLogSugared(logger.Sugar(), "")
			infoLog(logger.Sugar().Desugar(), "")

			entries := logs.AllUntimed()
			assert.Equal(t, 3, len(entries), "Unexpected number of logs written out.")
			for _, entry := range []observer.LoggedEntry{entries[0], entries[2]} {
				assert.Regexp(
					t,
					tt.loggerFunction,
					entry.Caller.Function,
					"Expected to find function name in output.",
				)
			}
			assert.Regexp(
				t,
				tt.sugaredFunction,
				entries[1].Caller.Function,
				"Expected to find function name in output.",
			)
		})
	}
}

func TestLoggerAddCallerFail(t *testing.T) {
	errBuf := &ztest.Buffer{}
	withLogger(t, DebugLevel, opts(AddCaller(), AddCallerSkip(1e3), ErrorOutput(errBuf)), func(log *Logger, logs *observer.ObservedLogs) {
		log.Info("Failure.")
		assert.Regexp(
			t,
			`Logger.check error: failed to get caller`,
			errBuf.String(),
			"Didn't find expected failure message.",
		)
		assert.Equal(
			t,
			logs.AllUntimed()[0].Message,
			"Failure.",
			"Expected original message to survive failures in runtime.Caller.")
		assert.Equal(
			t,
			logs.AllUntimed()[0].Caller.Function,
			"",
			"Expected function name to be empty string.")
	})
}

func TestLoggerReplaceCore(t *testing.T) {
	replace := WrapCore(func(zapcore.Core) zapcore.Core {
		return zapcore.NewNopCore()
	})
	withLogger(t, DebugLevel, opts(replace), func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Debug("")
		logger.Info("")
		logger.Warn("")
		assert.Equal(t, 0, logs.Len(), "Expected no-op core to write no logs.")
	})
}

func TestLoggerIncreaseLevel(t *testing.T) {
	withLogger(t, DebugLevel, opts(IncreaseLevel(WarnLevel)), func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Info("logger.Info")
		logger.Warn("logger.Warn")
		logger.Error("logger.Error")
		require.Equal(t, 2, logs.Len(), "expected only warn + error logs due to IncreaseLevel.")
		assert.Equal(
			t,
			logs.AllUntimed()[0].Message,
			"logger.Warn",
			"Expected first logged message to be warn level message",
		)
	})
}

func TestLoggerHooks(t *testing.T) {
	hook, seen := makeCountingHook()
	withLogger(t, DebugLevel, opts(Hooks(hook)), func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Debug("")
		logger.Info("")
	})
	assert.Equal(t, int64(2), seen.Load(), "Hook saw an unexpected number of logs.")
}

func TestLoggerConcurrent(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(logger *Logger, logs *observer.ObservedLogs) {
		child := logger.With(String("foo", "bar"))

		wg := &sync.WaitGroup{}
		runConcurrently(5, 10, wg, func() {
			logger.Info("", String("foo", "bar"))
		})
		runConcurrently(5, 10, wg, func() {
			child.Info("")
		})

		wg.Wait()

		// Make sure the output doesn't contain interspersed entries.
		assert.Equal(t, 100, logs.Len(), "Unexpected number of logs written out.")
		for _, obs := range logs.AllUntimed() {
			assert.Equal(
				t,
				observer.LoggedEntry{
					Entry:   zapcore.Entry{Level: InfoLevel},
					Context: []Field{String("foo", "bar")},
				},
				obs,
				"Unexpected log output.",
			)
		}
	})
}

func TestLoggerFatalOnNoop(t *testing.T) {
	exitStub := exit.Stub()
	defer exitStub.Unstub()
	core, _ := observer.New(InfoLevel)

	// We don't allow a no-op fatal hook.
	New(core, WithFatalHook(zapcore.WriteThenNoop)).Fatal("great sadness")
	assert.True(t, exitStub.Exited, "must exit for WriteThenNoop")
	assert.Equal(t, 1, exitStub.Code, "must exit with status 1 for WriteThenNoop")
}

func TestLoggerCustomOnPanic(t *testing.T) {
	tests := []struct {
		msg          string
		level        zapcore.Level
		opts         []Option
		finished     bool
		want         []observer.LoggedEntry
		recoverValue any
	}{
		{
			msg:      "panic with nil hook",
			level:    PanicLevel,
			opts:     opts(WithPanicHook(nil)),
			finished: false,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: PanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: "foobar",
		},
		{
			msg:      "panic with noop hook",
			level:    PanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenNoop)),
			finished: false,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: PanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: "foobar",
		},
		{
			msg:      "no panic with goexit hook",
			level:    PanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenGoexit)),
			finished: false,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: PanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: nil,
		},
		{
			msg:      "dpanic no panic in development mode with goexit hook",
			level:    DPanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenGoexit), Development()),
			finished: false,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: DPanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: nil,
		},
		{
			msg:      "dpanic panic in development mode with noop hook",
			level:    DPanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenNoop), Development()),
			finished: false,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: DPanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: "foobar",
		},
		{
			msg:      "dpanic no exit in production mode with goexit hook",
			level:    DPanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenPanic)),
			finished: true,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: DPanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: nil,
		},
		{
			msg:      "dpanic no panic in production mode with panic hook",
			level:    DPanicLevel,
			opts:     opts(WithPanicHook(zapcore.WriteThenPanic)),
			finished: true,
			want: []observer.LoggedEntry{
				{
					Entry:   zapcore.Entry{Level: DPanicLevel, Message: "foobar"},
					Context: []Field{},
				},
			},
			recoverValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			withLogger(t, InfoLevel, tt.opts, func(logger *Logger, logs *observer.ObservedLogs) {
				var finished bool
				recovered := make(chan any)
				go func() {
					defer func() {
						recovered <- recover()
					}()

					logger.Log(tt.level, "foobar")
					finished = true
				}()

				assert.Equal(t, tt.recoverValue, <-recovered, "unexpected value from recover()")
				assert.Equal(t, tt.finished, finished, "expect goroutine finished state doesn't match")
				assert.Equal(t, tt.want, logs.AllUntimed(), "unexpected logs")
			})
		})
	}
}

func TestLoggerCustomOnFatal(t *testing.T) {
	tests := []struct {
		msg          string
		onFatal      zapcore.CheckWriteAction
		recoverValue interface{}
	}{
		{
			msg:          "panic",
			onFatal:      zapcore.WriteThenPanic,
			recoverValue: "fatal",
		},
		{
			msg:          "goexit",
			onFatal:      zapcore.WriteThenGoexit,
			recoverValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			withLogger(t, InfoLevel, opts(OnFatal(tt.onFatal)), func(logger *Logger, logs *observer.ObservedLogs) {
				var finished bool
				recovered := make(chan interface{})
				go func() {
					defer func() {
						recovered <- recover()
					}()

					logger.Fatal("fatal")
					finished = true
				}()

				assert.Equal(t, tt.recoverValue, <-recovered, "unexpected value from recover()")
				assert.False(t, finished, "expect goroutine to not finish after Fatal")

				assert.Equal(t, []observer.LoggedEntry{{
					Entry:   zapcore.Entry{Level: FatalLevel, Message: "fatal"},
					Context: []Field{},
				}}, logs.AllUntimed(), "unexpected logs")
			})
		})
	}
}

type customWriteHook struct {
	called bool
}

func (h *customWriteHook) OnWrite(_ *zapcore.CheckedEntry, _ []Field) {
	h.called = true
}

func TestLoggerWithFatalHook(t *testing.T) {
	var h customWriteHook
	withLogger(t, InfoLevel, opts(WithFatalHook(&h)), func(logger *Logger, logs *observer.ObservedLogs) {
		logger.Fatal("great sadness")
		assert.True(t, h.called)
		assert.Equal(t, 1, logs.FilterLevelExact(FatalLevel).Len())
	})
}

func TestNopLogger(t *testing.T) {
	logger := NewNop()

	t.Run("basic levels", func(t *testing.T) {
		logger.Debug("foo", String("k", "v"))
		logger.Info("bar", Int("x", 42))
		logger.Warn("baz", Strings("ks", []string{"a", "b"}))
		logger.Error("qux", Error(errors.New("great sadness")))
	})

	t.Run("DPanic", func(t *testing.T) {
		logger.With(String("component", "whatever")).DPanic("stuff")
	})

	t.Run("Panic", func(t *testing.T) {
		assert.Panics(t, func() {
			logger.Panic("great sadness")
		}, "Nop logger should still cause panics.")
	})
}

func TestMust(t *testing.T) {
	t.Run("must without an error does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() { Must(NewNop(), nil) }, "must paniced with no error")
	})

	t.Run("must with an error panics", func(t *testing.T) {
		assert.Panics(t, func() { Must(nil, errors.New("an error")) }, "must did not panic with an error")
	})
}

func infoLog(logger *Logger, msg string, fields ...Field) {
	logger.Info(msg, fields...)
}

func infoLogSugared(logger *SugaredLogger, args ...interface{}) {
	logger.Info(args...)
}

func loggerLevelStats(
	t *testing.T,
	snap zapcore.SamplerSnapshot,
	lvl zapcore.Level,
) zapcore.SamplerLevelStats {
	t.Helper()
	for _, s := range snap.Stats {
		if s.Level == lvl {
			return s
		}
	}
	t.Fatalf("level %v not found in sampler snapshot", lvl)
	return zapcore.SamplerLevelStats{}
}

func newSamplerLogger(
	enab zapcore.LevelEnabler,
	tick time.Duration,
	first, thereafter int,
	opts ...Option,
) (*Logger, *observer.ObservedLogs) {
	core, logs := observer.New(enab)
	sampled := zapcore.NewSamplerWithOptions(core, tick, first, thereafter)
	return New(sampled, opts...), logs
}

func TestLoggerSamplerSnapshot(t *testing.T) {
	log, logs := newSamplerLogger(zapcore.DebugLevel, time.Minute, 2, 0)

	snap, ok := log.SamplerSnapshot()
	require.True(t, ok, "sampling logger must report a snapshot")
	assert.Equal(t, zapcore.SamplerConfigSnapshot{
		Tick: time.Minute, First: 2, Thereafter: 0,
	}, snap.Config, "unexpected config snapshot")

	// Non-sampling loggers don't support the sampler snapshot.
	_, ok = NewNop().SamplerSnapshot()
	assert.False(t, ok, "nop logger must not report sampler stats")
	assert.False(t, NewNop().UpdateSamplerConfig(time.Second, 1, 1),
		"nop logger must not support sampler reconfiguration")

	// 5 info attempts in one window: 2 accepted, 3 dropped, 1 reset.
	for i := 0; i < 5; i++ {
		log.Info("burst")
	}
	snap, ok = log.SamplerSnapshot()
	require.True(t, ok)
	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.InfoLevel, Accepted: 2, Dropped: 3, Resets: 1,
	}, loggerLevelStats(t, snap, zapcore.InfoLevel))
	assert.Len(t, logs.TakeAll(), 2, "unexpected number of logs written")

	// Dynamic reconfiguration is reflected by the next snapshot.
	require.True(t, log.UpdateSamplerConfig(time.Hour, 9, 4))
	snap, _ = log.SamplerSnapshot()
	assert.Equal(t, zapcore.SamplerConfigSnapshot{
		Tick: time.Hour, First: 9, Thereafter: 4,
	}, snap.Config, "config update must be visible atomically")
}

// fireDPanic invokes DPanic on the logger and reports whether it panicked,
// recovering so the caller can continue the test.
func fireDPanic(log *Logger) (panicked bool) {
	defer func() {
		panicked = recover() != nil
	}()
	log.DPanic("dpanic")
	return false
}

func TestLoggerSamplerDPanic(t *testing.T) {
	t.Run("production mode", func(t *testing.T) {
		// In production, DPanic is sampled like every other level and never
		// panics.
		log, logs := newSamplerLogger(zapcore.DebugLevel, time.Minute, 2, 0)

		assert.NotPanics(t, func() {
			for i := 0; i < 5; i++ {
				log.DPanic("dpanic")
			}
		})

		snap, ok := log.SamplerSnapshot()
		require.True(t, ok)
		assert.Equal(t, zapcore.SamplerLevelStats{
			Level: zapcore.DPanicLevel, Accepted: 2, Dropped: 3, Resets: 1,
		}, loggerLevelStats(t, snap, zapcore.DPanicLevel))

		written := logs.TakeAll()
		assert.Len(t, written, 2)
		for _, ent := range written {
			assert.Equal(t, zapcore.DPanicLevel, ent.Level)
		}
	})

	t.Run("development mode", func(t *testing.T) {
		// In development, DPanic panics even when the sampler drops the
		// entry: the drop is still counted, but nothing is written.
		log, logs := newSamplerLogger(
			zapcore.DebugLevel, time.Minute, 1, 1000, Development(),
		)

		for i := 0; i < 5; i++ {
			assert.True(t, fireDPanic(log),
				"DPanic must panic in development even when sampled away")
		}

		snap, ok := log.SamplerSnapshot()
		require.True(t, ok)
		assert.Equal(t, zapcore.SamplerLevelStats{
			Level: zapcore.DPanicLevel, Accepted: 1, Dropped: 4, Resets: 1,
		}, loggerLevelStats(t, snap, zapcore.DPanicLevel))

		written := logs.TakeAll()
		assert.Len(t, written, 1, "dropped DPanic entries must not be written")
		assert.Equal(t, zapcore.DPanicLevel, written[0].Level)
	})
}

func TestLoggerSamplerReplaceCore(t *testing.T) {
	logA, logsA := newSamplerLogger(zapcore.DebugLevel, time.Minute, 2, 0)
	for i := 0; i < 5; i++ {
		logA.Info("a")
	}
	snapA, ok := logA.SamplerSnapshot()
	require.True(t, ok)
	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.InfoLevel, Accepted: 2, Dropped: 3, Resets: 1,
	}, loggerLevelStats(t, snapA, zapcore.InfoLevel))

	coreB, logsB := observer.New(zapcore.DebugLevel)
	// Replace the whole core with a fresh sampler around a fresh core.
	logB := logA.WithOptions(WrapCore(func(_ zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(coreB, time.Minute, 2, 0)
	}))
	for i := 0; i < 3; i++ {
		logB.Info("b")
	}

	snapB, ok := logB.SamplerSnapshot()
	require.True(t, ok)
	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.InfoLevel, Accepted: 2, Dropped: 1, Resets: 1,
	}, loggerLevelStats(t, snapB, zapcore.InfoLevel),
		"replaced core must keep its own counters")

	// The original logger must be unaffected by the replacement.
	snapA, _ = logA.SamplerSnapshot()
	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.InfoLevel, Accepted: 2, Dropped: 3, Resets: 1,
	}, loggerLevelStats(t, snapA, zapcore.InfoLevel),
		"replacing a core must not mix counters into the old logger")
	assert.Len(t, logsA.TakeAll(), 2)
	assert.Len(t, logsB.TakeAll(), 2)

	// Runtime reconfiguration of one logger must not touch the other.
	require.True(t, logB.UpdateSamplerConfig(time.Hour, 9, 9))
	snapA, _ = logA.SamplerSnapshot()
	snapB, _ = logB.SamplerSnapshot()
	assert.Equal(t, time.Minute, snapA.Config.Tick, "logA config must not change")
	assert.Equal(t, uint64(2), snapA.Config.First)
	assert.Equal(t, time.Hour, snapB.Config.Tick, "logB config change must apply")
	assert.Equal(t, uint64(9), snapB.Config.First)

	// Replacing with a non-sampling core removes the reporting capability.
	coreC, _ := observer.New(zapcore.DebugLevel)
	logC := logA.WithOptions(WrapCore(func(_ zapcore.Core) zapcore.Core {
		return coreC
	}))
	_, ok = logC.SamplerSnapshot()
	assert.False(t, ok, "non-sampling replacement must not report sampler stats")
	assert.False(t, logC.UpdateSamplerConfig(time.Second, 1, 1))
}

func TestLoggerSamplerConcurrentIsolation(t *testing.T) {
	// Two loggers, two independent sampling cores. Concurrent logging must
	// attribute every entry to the logger that emitted it.
	logA, logsA := newSamplerLogger(zapcore.DebugLevel, time.Hour, 1_000_000, 1)
	logB, logsB := newSamplerLogger(zapcore.DebugLevel, time.Hour, 1_000_000, 1)

	const (
		goroutines = 8
		iterations = 200
		total      = goroutines * iterations
	)

	var wg sync.WaitGroup
	runConcurrently(goroutines, iterations, &wg, func() {
		logA.Info("alpha")
	})
	runConcurrently(goroutines, iterations, &wg, func() {
		logB.Warn("beta")
	})
	wg.Wait()

	snapA, ok := logA.SamplerSnapshot()
	require.True(t, ok)
	snapB, ok := logB.SamplerSnapshot()
	require.True(t, ok)

	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.InfoLevel, Accepted: total, Dropped: 0, Resets: 1,
	}, loggerLevelStats(t, snapA, zapcore.InfoLevel),
		"logA info counters must only count logA's info logs")
	assert.Equal(t, zapcore.SamplerLevelStats{
		Level: zapcore.WarnLevel, Accepted: total, Dropped: 0, Resets: 1,
	}, loggerLevelStats(t, snapB, zapcore.WarnLevel),
		"logB warn counters must only count logB's warn logs")

	// No level of either sampler may have picked up the other logger's
	// traffic.
	for _, s := range snapA.Stats {
		if s.Level != zapcore.InfoLevel {
			assert.Equal(t, zapcore.SamplerLevelStats{Level: s.Level}, s,
				"logA must have no counters at %v", s.Level)
		}
	}
	for _, s := range snapB.Stats {
		if s.Level != zapcore.WarnLevel {
			assert.Equal(t, zapcore.SamplerLevelStats{Level: s.Level}, s,
				"logB must have no counters at %v", s.Level)
		}
	}

	assert.Equal(t, total, logsA.Len(), "unexpected number of logs from logA")
	assert.Equal(t, total, logsB.Len(), "unexpected number of logs from logB")
}
