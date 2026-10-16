package reverseproxy

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestProbeRegistryOnlyRemovedUpstream(t *testing.T) {
	a1, a2 := net.Pipe()
	b1, b2 := net.Pipe()
	defer a2.Close()
	defer b2.Close()
	sa := &detachedStream{conns: []openConnection{{conn: a1}}, stats: newStreamStats("pa", nil, zapcore.DebugLevel)}
	sb := &detachedStream{conns: []openConnection{{conn: b1}}, stats: newStreamStats("pb", nil, zapcore.DebugLevel)}
	r := &detachedRegistry{streams: make(map[string]map[*detachedStream]struct{})}
	r.add("pa", sa)
	r.add("pb", sb)
	n, _ := r.closeUpstream("pa")
	if n != 1 || r.count("pa") != 0 || r.count("pb") != 1 {
		t.Fatalf("n=%d pa=%d pb=%d", n, r.count("pa"), r.count("pb"))
	}
	a2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := a2.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected closed")
	}
	sa.stats.finish()
	sb.stats.finish()
}

func TestProbeParseAndDefaults(t *testing.T) {
	h := &Handler{}
	if err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser("reverse_proxy a:1 {\n stream_detached\n stream_logs {\n skip_handshake\n level warn\n logger_name access\n }\n}")); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(h)
	var h2 Handler
	if err := json.Unmarshal(b, &h2); err != nil {
		t.Fatal(err)
	}
	if !h2.StreamDetached || h2.StreamLogs == nil || !h2.StreamLogs.SkipHandshake || h2.StreamLogs.LoggerName != "access" {
		t.Fatalf("%+v", h2)
	}
	if h2.streamLevel() != zapcore.WarnLevel {
		t.Fatal("level")
	}
	if (&Handler{}).streamLevel() != zapcore.DebugLevel {
		t.Fatal("default level")
	}
	bad := &Handler{StreamLogs: &StreamLogs{Level: "bogus"}}
	if bad.provisionStreamLogs() == nil {
		t.Fatal("expected error")
	}
}

func TestProbeFinishOnceConcurrent(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	s := newStreamStats("u", []*zap.Logger{zap.New(core)}, zapcore.InfoLevel)
	s.toBackend.Add(5)
	s.fromBackend.Add(7)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.setResult("cleanup"); s.finish() }()
	}
	wg.Wait()
	if logs.Len() != 1 {
		t.Fatalf("logs=%d", logs.Len())
	}
	e := logs.All()[0]
	if e.Level != zapcore.InfoLevel || e.ContextMap()["bytes_to_backend"] != int64(5) || e.ContextMap()["bytes_from_backend"] != int64(7) {
		t.Fatalf("%v", e)
	}
}
