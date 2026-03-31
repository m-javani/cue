// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"testing"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapRaftLogger_ImplementsInterface(t *testing.T) {
	// Compile-time check
	var _ raft.Logger = NewRaftZapLogger(zap.NewNop())
}

func TestZapRaftLogger_DelegatesToZap(t *testing.T) {
	tests := []struct {
		name       string
		method     func(logger *RaftZapAdapter, args ...any)
		format     func(logger *RaftZapAdapter, format string, args ...any)
		args       []any
		formatMsg  string
		formatArgs []any
		expected   string
	}{
		{
			name:       "Debug",
			method:     func(l *RaftZapAdapter, args ...any) { l.Debug(args...) },
			format:     func(l *RaftZapAdapter, format string, args ...any) { l.Debugf(format, args...) },
			args:       []any{"test debug"},
			formatMsg:  "test format %s",
			formatArgs: []any{"world"},
			expected:   "test debug",
		},
		{
			name:       "Info",
			method:     func(l *RaftZapAdapter, args ...any) { l.Info(args...) },
			format:     func(l *RaftZapAdapter, format string, args ...any) { l.Infof(format, args...) },
			args:       []any{"test info"},
			formatMsg:  "test format %s",
			formatArgs: []any{"world"},
			expected:   "test info",
		},
		{
			name:       "Warning",
			method:     func(l *RaftZapAdapter, args ...any) { l.Warning(args...) },
			format:     func(l *RaftZapAdapter, format string, args ...any) { l.Warningf(format, args...) },
			args:       []any{"test warning"},
			formatMsg:  "test format %s",
			formatArgs: []any{"world"},
			expected:   "test warning",
		},
		{
			name:       "Error",
			method:     func(l *RaftZapAdapter, args ...any) { l.Error(args...) },
			format:     func(l *RaftZapAdapter, format string, args ...any) { l.Errorf(format, args...) },
			args:       []any{"test error"},
			formatMsg:  "test format %s",
			formatArgs: []any{"world"},
			expected:   "test error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test regular method
			core, observed := observer.New(zap.DebugLevel)
			lg := zap.New(core)
			logger := &RaftZapAdapter{lg: lg}

			tt.method(logger, tt.args...)
			logs := observed.All()
			if len(logs) != 1 {
				t.Fatalf("expected 1 log, got %d", len(logs))
			}
			if logs[0].Message != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, logs[0].Message)
			}

			// Test formatted method
			core, observed = observer.New(zap.DebugLevel)
			lg = zap.New(core)
			logger = &RaftZapAdapter{lg: lg}

			tt.format(logger, tt.formatMsg, tt.formatArgs...)
			logs = observed.All()
			if len(logs) != 1 {
				t.Fatalf("expected 1 log, got %d", len(logs))
			}
			expectedFormatted := "test format world"
			if logs[0].Message != expectedFormatted {
				t.Errorf("expected %q, got %q", expectedFormatted, logs[0].Message)
			}
		})
	}
}

func TestRaftZapAdapter_WithFields(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	lg := zap.New(core)
	logger := &RaftZapAdapter{lg: lg}

	// Test with structured fields
	logger.lg = lg.With(zap.String("key", "value"))
	logger.Info("test with fields")

	logs := observed.All()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	if logs[0].Context[0].Key != "key" || logs[0].Context[0].String != "value" {
		t.Errorf("expected field key=value, got %v", logs[0].Context)
	}
}
