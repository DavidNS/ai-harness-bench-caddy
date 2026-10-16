package integration

import (
	"fmt"
	"testing"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func TestProbeUpgradeEncodeInterceptDetached(t *testing.T) {
	startUpgradeBackend(t, streamBackendA)

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`{
		skip_install_trust
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	localhost:9080 {
		route {
			encode gzip
			intercept {
				@sw status 101
				handle_response @sw {
					respond "replaced" 200
				}
			}
			reverse_proxy %s {
				stream_detached
			}
		}
	}`, streamBackendA), "caddyfile")

	tn := openTunnel(t, "/ws", "websocket", "Accept-Encoding: gzip", "Sec-WebSocket-Version: 13", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==")
	tn.echo("one")
	tn.echo("two")
}
