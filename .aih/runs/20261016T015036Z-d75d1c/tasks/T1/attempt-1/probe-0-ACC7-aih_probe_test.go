package caddyhttp

import (
	"net"
	"testing"
)

func TestProbeUnwrapPlain(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if UnwrapHijackedConn(a) != a {
		t.Fatal("plain conn must be unchanged")
	}
}
