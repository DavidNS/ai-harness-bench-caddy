package reverseproxy

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestProbeCloseLog(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	s := newStreamStats("u", []*zap.Logger{zap.New(core)}, zapcore.InfoLevel)
	s.toBackend.Add(5)
	s.fromBackend.Add(7)
	s.finish()
	e := logs.All()[0]
	if e.Level != zapcore.InfoLevel || e.ContextMap()["bytes_to_backend"] != int64(5) {
		t.Fatalf("%v", e)
	}
}
