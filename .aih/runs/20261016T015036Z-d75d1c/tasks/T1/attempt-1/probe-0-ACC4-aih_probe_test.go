package reverseproxy

import (
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestProbeDefaults(t *testing.T) {
	if (&Handler{}).streamLevel() != zapcore.DebugLevel {
		t.Fatal("default level")
	}
	bad := &Handler{StreamLogs: &StreamLogs{Level: "bogus"}}
	if bad.provisionStreamLogs() == nil {
		t.Fatal("expected error")
	}
}
