package reverseproxy

import (
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestAihProbeLevelDefault(t *testing.T) {
	l, err := parseStreamLogLevel("")
	if err != nil || l != zapcore.DebugLevel {
		t.Fatal(l, err)
	}
}
