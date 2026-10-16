package reverseproxy

import (
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestProbeFinishOnceConcurrent(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	s := newStreamStats("u", []*zap.Logger{zap.New(core)}, zapcore.InfoLevel)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.setResult("cleanup"); s.finish() }()
	}
	wg.Wait()
	if logs.Len() != 1 {
		t.Fatalf("logs=%d", logs.Len())
	}
}
