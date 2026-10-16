package reverseproxy

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestAihProbeStreamMetricsConcurrent(t *testing.T) {
	const up = "aih-probe-metrics:1"
	st := newUpgradedStream(up)
	st.toBackend.Add(7)
	st.fromBackend.Add(9)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				st.markResult(streamResultReload)
			}
			st.finish(streamResultError)
		}()
	}
	wg.Wait()
	read := func(m prometheus.Metric) *dto.Metric {
		o := new(dto.Metric)
		_ = m.Write(o)
		return o
	}
	if v := read(streamMetrics.active.WithLabelValues(up)).GetGauge().GetValue(); v != 0 {
		t.Errorf("active %v", v)
	}
	total := 0.0
	for _, r := range []string{"closed", "error", "timeout", "reload", "upstream_removed"} {
		total += read(streamMetrics.closedBy.WithLabelValues(up, r)).GetCounter().GetValue()
	}
	if total != 1 {
		t.Errorf("closed total %v", total)
	}
	if v := read(streamMetrics.bytes.WithLabelValues(up, "to_backend")).GetCounter().GetValue(); v != 7 {
		t.Errorf("to_backend %v", v)
	}
	if v := read(streamMetrics.bytes.WithLabelValues(up, "from_backend")).GetCounter().GetValue(); v != 9 {
		t.Errorf("from_backend %v", v)
	}
}

func TestAihProbeCaddyfileJSONAndErrors(t *testing.T) {
	h := new(Handler)
	if err := h.UnmarshalCaddyfile(caddyfile.NewTestDispenser("reverse_proxy a:1 {\nstream_detached\nstream_logs {\nskip_handshake\nlevel info\n}\n}")); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(h)
	for _, want := range []string{`"stream_detached":true`, `"skip_handshake":true`, `"level":"info"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("%s missing in %s", want, b)
		}
	}
	for _, bad := range []string{
		"stream_detached yes", "stream_logs {\nlevel nope\n}", "stream_logs {\nfoo\n}", "stream_logs {\nlevel\n}",
	} {
		if err := new(Handler).UnmarshalCaddyfile(caddyfile.NewTestDispenser("reverse_proxy a:1 {\n" + bad + "\n}")); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}
