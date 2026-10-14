package reverseproxy

import (
	"encoding/json"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestUnmarshalCaddyfileStreamOptions(t *testing.T) {
	for i, tc := range []struct {
		input        string
		wantDetached bool
		wantLogs     *StreamLogs
		wantErr      bool
	}{
		{input: `reverse_proxy localhost:8080`},
		{
			input: `reverse_proxy localhost:8080 {
				stream_detached
			}`,
			wantDetached: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					skip_handshake
				}
			}`,
			wantLogs: &StreamLogs{SkipHandshake: true},
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					level INFO
				}
			}`,
			wantLogs: &StreamLogs{Level: "info"},
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_detached
				stream_logs {
					skip_handshake
					level warn
				}
			}`,
			wantDetached: true,
			wantLogs:     &StreamLogs{SkipHandshake: true, Level: "warn"},
		},
		{
			// an empty block is accepted and leaves the defaults
			input: `reverse_proxy localhost:8080 {
				stream_logs
			}`,
			wantLogs: &StreamLogs{},
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_detached true
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs extra {
					skip_handshake
				}
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					skip_handshake true
				}
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					level
				}
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					level loud
				}
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					level info debug
				}
			}`,
			wantErr: true,
		},
		{
			input: `reverse_proxy localhost:8080 {
				stream_logs {
					bogus
				}
			}`,
			wantErr: true,
		},
	} {
		var h Handler
		err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))
		if tc.wantErr {
			if err == nil {
				t.Errorf("Test %d: expected error but got none", i)
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
		switch {
		case tc.wantLogs == nil && h.StreamLogs != nil:
			t.Errorf("Test %d: StreamLogs = %+v, want nil", i, *h.StreamLogs)
		case tc.wantLogs != nil && h.StreamLogs == nil:
			t.Errorf("Test %d: StreamLogs = nil, want %+v", i, *tc.wantLogs)
		case tc.wantLogs != nil && *h.StreamLogs != *tc.wantLogs:
			t.Errorf("Test %d: StreamLogs = %+v, want %+v", i, *h.StreamLogs, *tc.wantLogs)
		}
	}
}

func TestStreamOptionsJSONOmitsDefaults(t *testing.T) {
	b, err := json.Marshal(Handler{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"stream_detached", "stream_logs"} {
		if _, ok := got[key]; ok {
			t.Errorf("default config marshals %q: %s", key, b)
		}
	}

	b, err = json.Marshal(Handler{StreamDetached: true, StreamLogs: &StreamLogs{SkipHandshake: true, Level: "info"}})
	if err != nil {
		t.Fatal(err)
	}
	var h Handler
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatal(err)
	}
	if !h.StreamDetached || h.StreamLogs == nil || !h.StreamLogs.SkipHandshake || h.StreamLogs.Level != "info" {
		t.Errorf("JSON round trip lost the stream options: %s", b)
	}
}
