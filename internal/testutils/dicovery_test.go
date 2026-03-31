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

package testutils

import (
	"context"
	"testing"
)

func TestTestAddressResolver_Resolve(t *testing.T) {
	resolver := TestAddressResolver{}
	ctx := context.Background()

	tests := []struct {
		name    string
		node    string
		want    string
		wantErr bool
	}{
		{
			name:    "valid node with single dash",
			node:    "NC78YT-49217",
			want:    "127.0.0.1:49217",
			wantErr: false,
		},
		{
			name:    "valid node with multiple dashes",
			node:    "user-node-id-8080",
			want:    "127.0.0.1:8080",
			wantErr: false,
		},
		{
			name:    "valid node with port 0",
			node:    "node-0",
			want:    "127.0.0.1:0",
			wantErr: false,
		},
		{
			name:    "valid node with port 65535",
			node:    "node-65535",
			want:    "127.0.0.1:65535",
			wantErr: false,
		},
		{
			name:    "no dash in node",
			node:    "invalidnode",
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty string",
			node:    "",
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalid port number",
			node:    "node-abc",
			want:    "",
			wantErr: true,
		},
		{
			name:    "port out of range - too high",
			node:    "node-100000",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolver.Resolve(ctx, tt.node)

			if (err != nil) != tt.wantErr {
				t.Errorf("Resolve() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("Resolve() = %v, want %v", got, tt.want)
			}
		})
	}
}
