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

package state

import (
	"testing"

	"github.com/m-javani/cue/internal/model"
)

func TestCommandRouter(t *testing.T) {
	router := NewCommandRouter()

	t.Run("GetChannel on empty router", func(t *testing.T) {
		ch, exists := router.GetChannel("nonexistent")
		if exists {
			t.Error("GetChannel should return false for nonexistent topic")
		}
		if ch != nil {
			t.Error("GetChannel should return nil channel for nonexistent topic")
		}
	})

	t.Run("Register and GetChannel", func(t *testing.T) {
		ch := make(chan model.Command)
		topic := "test-topic"

		router.Register(topic, ch)

		got, exists := router.GetChannel(topic)
		if !exists {
			t.Error("GetChannel should return true for registered topic")
		}
		if got != ch {
			t.Error("GetChannel returned wrong channel")
		}
	})

	t.Run("Register overwrites existing", func(t *testing.T) {
		topic := "overwrite-topic"
		ch1 := make(chan model.Command)
		ch2 := make(chan model.Command)

		router.Register(topic, ch1)
		router.Register(topic, ch2)

		got, _ := router.GetChannel(topic)
		if got != ch2 {
			t.Error("Register should overwrite existing channel")
		}
	})

	t.Run("Unregister", func(t *testing.T) {
		topic := "unregister-topic"
		ch := make(chan model.Command)

		router.Register(topic, ch)
		router.Unregister(topic)

		_, exists := router.GetChannel(topic)
		if exists {
			t.Error("GetChannel should return false after Unregister")
		}
	})

	t.Run("Unregister nonexistent", func(t *testing.T) {
		// Should not panic
		router.Unregister("nonexistent")
	})

	t.Run("Concurrent access", func(t *testing.T) {
		done := make(chan bool)
		for i := 0; i < 100; i++ {
			go func() {
				router.Register("concurrent", make(chan model.Command))
				router.GetChannel("concurrent")
				router.Unregister("concurrent")
				done <- true
			}()
		}
		for i := 0; i < 100; i++ {
			<-done
		}
	})
}

func TestHeartbeatRouter(t *testing.T) {
	router := NewHeartbeatRouter()

	t.Run("GetChannel on empty router", func(t *testing.T) {
		ch, exists := router.GetChannel("nonexistent")
		if exists {
			t.Error("GetChannel should return false for nonexistent topic")
		}
		if ch != nil {
			t.Error("GetChannel should return nil channel for nonexistent topic")
		}
	})

	t.Run("Register and GetChannel", func(t *testing.T) {
		ch := make(chan model.ProxyHeartbeat)
		topic := "test-topic"

		router.Register(topic, ch)

		got, exists := router.GetChannel(topic)
		if !exists {
			t.Error("GetChannel should return true for registered topic")
		}
		if got != ch {
			t.Error("GetChannel returned wrong channel")
		}
	})

	t.Run("Register overwrites existing", func(t *testing.T) {
		topic := "overwrite-topic"
		ch1 := make(chan model.ProxyHeartbeat)
		ch2 := make(chan model.ProxyHeartbeat)

		router.Register(topic, ch1)
		router.Register(topic, ch2)

		got, _ := router.GetChannel(topic)
		if got != ch2 {
			t.Error("Register should overwrite existing channel")
		}
	})

	t.Run("Unregister", func(t *testing.T) {
		topic := "unregister-topic"
		ch := make(chan model.ProxyHeartbeat)

		router.Register(topic, ch)
		router.Unregister(topic)

		_, exists := router.GetChannel(topic)
		if exists {
			t.Error("GetChannel should return false after Unregister")
		}
	})

	t.Run("Unregister nonexistent", func(t *testing.T) {
		// Should not panic
		router.Unregister("nonexistent")
	})

	t.Run("Concurrent access", func(t *testing.T) {
		done := make(chan bool)
		for i := 0; i < 100; i++ {
			go func() {
				router.Register("concurrent", make(chan model.ProxyHeartbeat))
				router.GetChannel("concurrent")
				router.Unregister("concurrent")
				done <- true
			}()
		}
		for i := 0; i < 100; i++ {
			<-done
		}
	})
}
