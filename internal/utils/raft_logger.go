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
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

type RaftZapAdapter struct {
	lg *zap.Logger
}

func (l *RaftZapAdapter) Debug(args ...any) { l.lg.Sugar().Debug(args...) }
func (l *RaftZapAdapter) Debugf(format string, args ...any) {
	l.lg.Sugar().Debugf(format, args...)
}
func (l *RaftZapAdapter) Info(args ...any) { l.lg.Sugar().Info(args...) }
func (l *RaftZapAdapter) Infof(format string, args ...any) {
	l.lg.Sugar().Infof(format, args...)
}
func (l *RaftZapAdapter) Warning(args ...any) { l.lg.Sugar().Warn(args...) }
func (l *RaftZapAdapter) Warningf(format string, args ...any) {
	l.lg.Sugar().Warnf(format, args...)
}
func (l *RaftZapAdapter) Error(args ...any) { l.lg.Sugar().Error(args...) }
func (l *RaftZapAdapter) Errorf(format string, args ...any) {
	l.lg.Sugar().Errorf(format, args...)
}
func (l *RaftZapAdapter) Fatal(args ...any) { l.lg.Sugar().Fatal(args...) }
func (l *RaftZapAdapter) Fatalf(format string, args ...any) {
	l.lg.Sugar().Fatalf(format, args...)
}
func (l *RaftZapAdapter) Panic(args ...any) { l.lg.Sugar().Panic(args...) }
func (l *RaftZapAdapter) Panicf(format string, args ...any) {
	l.lg.Sugar().Panicf(format, args...)
}

func NewRaftZapLogger(lg *zap.Logger) raft.Logger {
	return &RaftZapAdapter{lg: lg}
}
