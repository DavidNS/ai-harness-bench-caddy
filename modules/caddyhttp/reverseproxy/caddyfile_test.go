package reverseproxy

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestUnmarshalCaddyfileStreamDetached(t *testing.T) {
	for i, tc := range []struct {
		input        string
		wantErr      string
		wantDetached bool
		wantLogs     *StreamLogs
	}{
		{
			input: `reverse_proxy localhost:9000`,
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_detached
			}`,
			wantDetached: true,
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_detached yes
			}`,
			wantErr: "wrong argument count",
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
				}
			}`,
			wantLogs: &StreamLogs{},
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_detached
				stream_logs {
					skip_handshake
					level INFO
				}
			}`,
			wantDetached: true,
			wantLogs:     &StreamLogs{SkipHandshake: true, Level: "INFO"},
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
					skip_handshake
				}
				stream_logs {
					level warn
				}
			}`,
			wantLogs: &StreamLogs{SkipHandshake: true, Level: "warn"},
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs on
			}`,
			wantErr: "wrong argument count",
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
					skip_handshake yes
				}
			}`,
			wantErr: "wrong argument count",
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
					level
				}
			}`,
			wantErr: "wrong argument count",
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
					level INFO DEBUG
				}
			}`,
			wantErr: "wrong argument count",
		},
		{
			input: `reverse_proxy localhost:9000 {
				stream_logs {
					handshake
				}
			}`,
			wantErr: "unrecognized stream_logs subdirective: handshake",
		},
	} {
		var h Handler
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Test %d: expected error containing %q, got %v", i, tc.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
			continue
		}
		if h.StreamDetached != tc.wantDetached {
			t.Errorf("Test %d: StreamDetached = %t, want %t", i, h.StreamDetached, tc.wantDetached)
		}
		if !reflect.DeepEqual(h.StreamLogs, tc.wantLogs) {
			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.wantLogs)
		}
	}
}

// TestStreamLogsConfig checks the stream_logs options, as parsed from the
// Caddyfile and from JSON, and the level they resolve to.
func TestStreamLogsConfig(t *testing.T) {
	var fromCaddyfile Handler
	err := fromCaddyfile.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`reverse_proxy localhost:9000 {
		stream_detached
		stream_logs {
			skip_handshake
			level error
		}
	}`))
	if err != nil {
		t.Fatalf("parsing Caddyfile: %v", err)
	}

	var fromJSON Handler
	err = json.Unmarshal([]byte(`{
		"stream_detached": true,
		"stream_logs": {"skip_handshake": true, "level": "error"}
	}`), &fromJSON)
	if err != nil {
		t.Fatalf("parsing JSON: %v", err)
	}

	for name, h := range map[string]Handler{"caddyfile": fromCaddyfile, "json": fromJSON} {
		if !h.StreamDetached {
			t.Errorf("%s: StreamDetached not set", name)
		}
		want := &StreamLogs{SkipHandshake: true, Level: "error"}
		if !reflect.DeepEqual(h.StreamLogs, want) {
			t.Errorf("%s: StreamLogs = %+v, want %+v", name, h.StreamLogs, want)
		}
	}

	out, err := json.Marshal(fromCaddyfile.StreamLogs)
	if err != nil {
		t.Fatalf("encoding JSON: %v", err)
	}
	if got, want := string(out), `{"skip_handshake":true,"level":"error"}`; got != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}

	for i, tc := range []struct {
		logs    *StreamLogs
		want    zapcore.Level
		wantErr bool
	}{
		{logs: nil, want: zapcore.DebugLevel},
		{logs: &StreamLogs{}, want: zapcore.DebugLevel},
		{logs: &StreamLogs{SkipHandshake: true}, want: zapcore.DebugLevel},
		{logs: &StreamLogs{Level: "INFO"}, want: zapcore.InfoLevel},
		{logs: &StreamLogs{Level: "error"}, want: zapcore.ErrorLevel},
		{logs: &StreamLogs{Level: "loud"}, want: zapcore.DebugLevel, wantErr: true},
	} {
		got, err := tc.logs.zapLevel()
		if (err != nil) != tc.wantErr {
			t.Errorf("Test %d: zapLevel() error = %v, wantErr %t", i, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("Test %d: zapLevel() = %v, want %v", i, got, tc.want)
		}

		h := Handler{StreamLogs: tc.logs}
		if err := h.Validate(); (err != nil) != tc.wantErr {
			t.Errorf("Test %d: Validate() error = %v, wantErr %t", i, err, tc.wantErr)
		}
	}
}
