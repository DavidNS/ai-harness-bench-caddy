package reverseproxy

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestStreamOptionsCaddyfile(t *testing.T) {
	for i, tc := range []struct {
		input        string
		wantDetached bool
		wantLogs     *StreamLogs
		wantErr      bool
	}{
		{input: `reverse_proxy localhost:9080`},
		{
			input: `reverse_proxy localhost:9080 {
				stream_detached
			}`,
			wantDetached: true,
		},
		{
			input: `reverse_proxy localhost:9080 {
				stream_logs {
					skip_handshake
					level INFO
					logger_name access
				}
			}`,
			wantLogs: &StreamLogs{SkipHandshake: true, Level: "INFO", LoggerName: "access"},
		},
		{
			// only the given options are set, the others keep their defaults
			input: `reverse_proxy localhost:9080 {
				stream_detached
				stream_logs {
					level warn
				}
			}`,
			wantDetached: true,
			wantLogs:     &StreamLogs{Level: "warn"},
		},
		{
			input: `reverse_proxy localhost:9080 {
				stream_logs {
				}
			}`,
			wantLogs: &StreamLogs{},
		},
		{input: "reverse_proxy localhost:9080 {\n stream_detached yes\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs inline {\n skip_handshake\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n skip_handshake yes\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n level\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n level INFO DEBUG\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n logger_name\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n logger_name access error\n}\n}", wantErr: true},
		{input: "reverse_proxy localhost:9080 {\n stream_logs {\n unknown\n}\n}", wantErr: true},
	} {
		h := &Handler{}
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
		if h.StreamDetached != tc.wantDetached {
			t.Errorf("Test %d: StreamDetached = %v, want %v", i, h.StreamDetached, tc.wantDetached)
		}
		if !reflect.DeepEqual(h.StreamLogs, tc.wantLogs) {
			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, h.StreamLogs, tc.wantLogs)
		}
	}
}

func TestStreamOptionsJSON(t *testing.T) {
	const in = `{"stream_detached":true,"stream_logs":{"skip_handshake":true,"level":"INFO","logger_name":"access"}}`

	var h Handler
	if err := json.Unmarshal([]byte(in), &h); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !h.StreamDetached {
		t.Error("StreamDetached = false, want true")
	}
	want := &StreamLogs{SkipHandshake: true, Level: "INFO", LoggerName: "access"}
	if !reflect.DeepEqual(h.StreamLogs, want) {
		t.Errorf("StreamLogs = %+v, want %+v", h.StreamLogs, want)
	}

	out, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Handler
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("unmarshal of marshaled handler: %v", err)
	}
	if !back.StreamDetached || !reflect.DeepEqual(back.StreamLogs, want) {
		t.Errorf("round trip lost the stream options: detached=%v logs=%+v", back.StreamDetached, back.StreamLogs)
	}

	// the defaults are not written out
	out, err = json.Marshal(Handler{})
	if err != nil {
		t.Fatalf("marshal defaults: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["stream_detached"]; ok {
		t.Error("stream_detached is marshaled although unset")
	}
	if _, ok := m["stream_logs"]; ok {
		t.Error("stream_logs is marshaled although unset")
	}
}
