package integration

import (
	"net/http"
	"testing"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func TestIntercept(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
			skip_install_trust
			admin localhost:2999
			http_port     9080
			https_port    9443
			grace_period  1ns
		}
	
		localhost:9080 {
			respond /intercept "I'm a teapot" 408
			header /intercept To-Intercept ok
			respond /no-intercept "I'm not a teapot"

			intercept {
				@teapot status 408
				handle_response @teapot {
					header /intercept intercepted {resp.header.To-Intercept}
					respond /intercept "I'm a combined coffee/tea pot that is temporarily out of coffee" 503
				}
			}	
		}
		`, "caddyfile")

	r, _ := tester.AssertGetResponse("http://localhost:9080/intercept", 503, "I'm a combined coffee/tea pot that is temporarily out of coffee")
	if r.Header.Get("intercepted") != "ok" {
		t.Fatalf(`header "intercepted" value is not "ok": %s`, r.Header.Get("intercepted"))
	}

	tester.AssertGetResponse("http://localhost:9080/no-intercept", 200, "I'm not a teapot")
}

// An upgrade is not a response to intercept: a catch-all handle_response must
// leave a 101 alone (the connection is hijacked, nothing may be written to
// it afterwards), while it still applies to every other response.
func TestInterceptUpgrade(t *testing.T) {
	for name, options := range map[string]string{
		"default":  "",
		"detached": "stream_detached",
	} {
		t.Run(name, func(t *testing.T) {
			newUpgradeBackend(t, "127.0.0.1:9081")
			path := logFile(t)
			tester := caddytest.NewTester(t)
			tester.InitServer(streamGlobalOptions+`
			localhost:9080 {
				log {
					output file "`+path+`"
				}
				route {
					intercept {
						handle_response {
							respond "intercepted" 203
						}
					}
					reverse_proxy 127.0.0.1:9081 {
						`+options+`
					}
				}
			}`, "caddyfile")

			tester.AssertGetResponse("http://localhost:9080/plain", 203, "intercepted")

			ws := dialWS(t, "/ws")
			ws.echo(t, "through intercept")
			ws.echo(t, "and again")
			ws.conn.Close()

			eventually(t, "the upgrade to be logged", func() bool {
				return len(findLog(path, "handled request", "/ws")) == 1
			})
			if status := findLog(path, "handled request", "/ws")[0]["status"]; status != float64(http.StatusSwitchingProtocols) {
				t.Errorf("logged status = %v, want 101", status)
			}
		})
	}
}

func TestInterceptReplaceStatusWithMatcher(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
		grace_period  1ns
	}

	localhost:9080 {
		respond /error "boom" 500

		intercept {
			@err status 5xx
			replace_status @err 200
		}
	}
	`, "caddyfile")

	tester.AssertGetResponse("http://localhost:9080/error", 200, "boom")
}

func TestInterceptReplaceStatusWithoutMatcher(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
		grace_period  1ns
	}

	localhost:9080 {
		respond /forbidden "denied" 403

		intercept {
			replace_status 200
		}
	}
	`, "caddyfile")

	tester.AssertGetResponse("http://localhost:9080/forbidden", 200, "denied")
}

func TestInterceptReplaceStatusNotMatched(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
		grace_period  1ns
	}

	localhost:9080 {
		respond /ok "all good" 200

		intercept {
			@err status 5xx
			replace_status @err 503
		}
	}
	`, "caddyfile")

	// 200 does not match @err (5xx), so status should pass through unchanged
	tester.AssertGetResponse("http://localhost:9080/ok", 200, "all good")
}
