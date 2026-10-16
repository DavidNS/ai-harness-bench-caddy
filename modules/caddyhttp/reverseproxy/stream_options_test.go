package reverseproxy

import (
	"encoding/json"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestStreamOptionsCaddyfile(t *testing.T) {
	for i, tc := range []struct {
		input      string
		wantErr    bool
		detached   bool
		logs       *StreamLogs
		wantNoLogs bool
	}{
		{input: `reverse_proxy localhost:9000`, wantNoLogs: true},
		{input: `reverse_proxy localhost:9000 {
			stream_detached
		}`, detached: true, wantNoLogs: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
			}
		}`, logs: &StreamLogs{}},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
				skip_handshake
			}
		}`, logs: &StreamLogs{SkipHandshake: true}},
		{input: `reverse_proxy localhost:9000 {
			stream_detached
			stream_logs {
				skip_handshake
				level info
			}
		}`, detached: true, logs: &StreamLogs{SkipHandshake: true, Level: "info"}},
		{input: `reverse_proxy localhost:9000 {
			stream_detached on
		}`, wantErr: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs verbose
		}`, wantErr: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
				skip_handshake yes
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
				level
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
				level info debug
			}
		}`, wantErr: true},
		{input: `reverse_proxy localhost:9000 {
			stream_logs {
				unknown
			}
		}`, wantErr: true},
	} {
		var h Handler
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error but got none", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
			continue
		}
		if h.StreamDetached != tc.detached {
			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.detached)
		}
		if tc.wantNoLogs {
			if h.StreamLogs != nil {
				t.Errorf("Test %d: StreamLogs = %+v, want nil", i, h.StreamLogs)
			}
			continue
		}
		if h.StreamLogs == nil || *h.StreamLogs != *tc.logs {
			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.logs)
		}
	}
}

func TestStreamOptionsJSON(t *testing.T) {
	var defaults Handler
	if err := json.Unmarshal([]byte(`{}`), &defaults); err != nil {
		t.Fatal(err)
	}
	if defaults.StreamDetached || defaults.StreamLogs != nil {
		t.Errorf("defaults = %v, %+v; want not detached and no stream logs", defaults.StreamDetached, defaults.StreamLogs)
	}

	var h Handler
	err := json.Unmarshal([]byte(`{"stream_detached": true, "stream_logs": {"skip_handshake": true, "level": "warn"}}`), &h)
	if err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached {
		t.Error("StreamDetached = false, want true")
	}
	if h.StreamLogs == nil || !h.StreamLogs.SkipHandshake || h.StreamLogs.Level != "warn" {
		t.Errorf("StreamLogs = %+v, want skip_handshake and level warn", h.StreamLogs)
	}
}

func TestParseStreamLogLevel(t *testing.T) {
	for i, tc := range []struct {
		input   string
		want    zapcore.Level
		wantErr bool
	}{
		{input: "", want: zapcore.DebugLevel},
		{input: "debug", want: zapcore.DebugLevel},
		{input: "info", want: zapcore.InfoLevel},
		{input: "warn", want: zapcore.WarnLevel},
		{input: "error", want: zapcore.ErrorLevel},
		{input: "loud", wantErr: true},
	} {
		got, err := parseStreamLogLevel(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected an error but got none", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: unexpected error: %v", i, err)
		}
		if got != tc.want {
			t.Errorf("Test %d: level = %v, want %v", i, got, tc.want)
		}
	}
}

func TestStreamLogLevelDefaultsToDebug(t *testing.T) {
	if got := (&Handler{}).streamLogLevel(); got != zapcore.DebugLevel {
		t.Errorf("default level = %v, want debug", got)
	}
	if got := (&Handler{StreamLogs: &StreamLogs{SkipHandshake: true}}).streamLogLevel(); got != zapcore.DebugLevel {
		t.Errorf("level without a configured one = %v, want debug", got)
	}
	if got := (&Handler{StreamLogs: &StreamLogs{Level: "error"}}).streamLogLevel(); got != zapcore.ErrorLevel {
		t.Errorf("configured level = %v, want error", got)
	}
}

func TestProvisionRejectsInvalidStreamLogLevel(t *testing.T) {
	h := &Handler{StreamLogs: &StreamLogs{Level: "loud"}}
	if err := h.Provision(caddy.Context{}); err == nil {
		t.Fatal("expected an error for an invalid stream log level")
	}
}
