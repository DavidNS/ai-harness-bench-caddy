package integration

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/net/http2/hpack"

	"github.com/caddyserver/caddy/v2/caddytest"
)

// (see https://github.com/caddyserver/caddy/issues/3556 for use case)
func TestH2ToH2CStream(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(` 
  {
	"admin": {
		"listen": "localhost:2999"
	},
    "apps": {
      "http": {
        "http_port": 9080,
        "https_port": 9443,
		"grace_period": 1,
        "servers": {
          "srv0": {
            "listen": [
              ":9443"
            ],
            "routes": [
              {
                "handle": [
                  {
                    "handler": "reverse_proxy",
                    "transport": {
                      "protocol": "http",
                      "compression": false,
                      "versions": [
                        "h2c",
                        "2"
                      ]
                    },
                    "upstreams": [
                      {
                        "dial": "localhost:54321"
                      }
                    ]
                  }
                ],
                "match": [
                  {
                    "path": [
                      "/tov2ray"
                    ]
                  }
                ]
              }
            ],
            "tls_connection_policies": [
              {
                "certificate_selection": {
                  "any_tag": ["cert0"]
                },
                "default_sni": "a.caddy.localhost"
              }
            ]
          }
        }
      },
      "tls": {
        "certificates": {
          "load_files": [
            {
              "certificate": "/a.caddy.localhost.crt",
              "key": "/a.caddy.localhost.key",
              "tags": [
                "cert0"
              ]
            }
          ]
        }
      },
      "pki": {
        "certificate_authorities" : {
          "local" : {
            "install_trust": false
          }
        }
      }
    }
  }
  `, "json")

	expectedBody := "some data to be echoed"
	// start the server
	server := testH2ToH2CStreamServeH2C(t)
	go server.ListenAndServe()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		server.Shutdown(ctx)
	}()

	r, w := io.Pipe()
	req := &http.Request{
		Method: "PUT",
		Body:   io.NopCloser(r),
		URL: &url.URL{
			Scheme: "https",
			Host:   "127.0.0.1:9443",
			Path:   "/tov2ray",
		},
		Proto:      "HTTP/2",
		ProtoMajor: 2,
		ProtoMinor: 0,
		Header:     make(http.Header),
	}
	// Disable any compression method from server.
	req.Header.Set("Accept-Encoding", "identity")

	resp := tester.AssertResponseCode(req, http.StatusOK)
	if resp.StatusCode != http.StatusOK {
		return
	}
	go func() {
		fmt.Fprint(w, expectedBody)
		w.Close()
	}()

	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unable to read the response body %s", err)
	}

	body := string(bytes)

	if !strings.Contains(body, expectedBody) {
		t.Errorf("requesting \"%s\" expected response body \"%s\" but got \"%s\"", req.RequestURI, expectedBody, body)
	}
}

func testH2ToH2CStreamServeH2C(t *testing.T) *http.Server {
	h2s := &http2.Server{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rstring, err := httputil.DumpRequest(r, false)
		if err == nil {
			t.Logf("h2c server received req: %s", rstring)
		}
		// We only accept HTTP/2!
		if r.ProtoMajor != 2 {
			t.Error("Not an HTTP/2 request, rejected!")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if r.Host != "127.0.0.1:9443" {
			t.Errorf("r.Host doesn't match, %v!", r.Host)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !strings.HasPrefix(r.URL.Path, "/tov2ray") {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		http.NewResponseController(w).Flush()

		buf := make([]byte, 4*1024)

		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
			}

			if err != nil {
				if err == io.EOF {
					r.Body.Close()
				}
				break
			}
		}
	})

	server := &http.Server{
		Addr:    "127.0.0.1:54321",
		Handler: h2c.NewHandler(handler, h2s),
	}
	return server
}

// (see https://github.com/caddyserver/caddy/issues/3606 for use case)
func TestH2ToH1ChunkedResponse(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(` 
{
	"admin": {
		"listen": "localhost:2999"
	},
  "logging": {
    "logs": {
      "default": {
        "level": "DEBUG"
      }
    }
  },
  "apps": {
    "http": {
      "http_port": 9080,
      "https_port": 9443,
	  "grace_period": 1,
      "servers": {
        "srv0": {
          "listen": [
            ":9443"
          ],
          "routes": [
            {
              "handle": [
                {
                  "handler": "subroute",
                  "routes": [
                    {
                      "handle": [
                        {
                          "encodings": {
                            "gzip": {}
                          },
                          "handler": "encode"
                        }
                      ]
                    },
                    {
                      "handle": [
                        {
                          "handler": "reverse_proxy",
                          "upstreams": [
                            {
                              "dial": "localhost:54321"
                            }
                          ]
                        }
                      ],
                      "match": [
                        {
                          "path": [
                            "/tov2ray"
                          ]
                        }
                      ]
                    }
                  ]
                }
              ],
              "terminal": true
            }
          ],
          "tls_connection_policies": [
            {
              "certificate_selection": {
                "any_tag": [
                  "cert0"
                ]
              },
              "default_sni": "a.caddy.localhost"
            }
          ]
        }
      }
    },
    "tls": {
      "certificates": {
        "load_files": [
          {
            "certificate": "/a.caddy.localhost.crt",
            "key": "/a.caddy.localhost.key",
            "tags": [
              "cert0"
            ]
          }
        ]
      }
    },
    "pki": {
      "certificate_authorities": {
        "local": {
          "install_trust": false
        }
      }
    }
  }
}
  `, "json")

	// need a large body here to trigger caddy's compression, larger than gzip.miniLength
	expectedBody, err := GenerateRandomString(1024)
	if err != nil {
		t.Fatalf("generate expected body failed, err: %s", err)
	}

	// start the server
	server := testH2ToH1ChunkedResponseServeH1(t)
	go server.ListenAndServe()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		server.Shutdown(ctx)
	}()

	r, w := io.Pipe()
	req := &http.Request{
		Method: "PUT",
		Body:   io.NopCloser(r),
		URL: &url.URL{
			Scheme: "https",
			Host:   "127.0.0.1:9443",
			Path:   "/tov2ray",
		},
		Proto:      "HTTP/2",
		ProtoMajor: 2,
		ProtoMinor: 0,
		Header:     make(http.Header),
	}
	// underlying transport will automatically add gzip
	// req.Header.Set("Accept-Encoding", "gzip")
	go func() {
		fmt.Fprint(w, expectedBody)
		w.Close()
	}()
	resp := tester.AssertResponseCode(req, http.StatusOK)
	if resp.StatusCode != http.StatusOK {
		return
	}

	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unable to read the response body %s", err)
	}

	body := string(bytes)

	if body != expectedBody {
		t.Errorf("requesting \"%s\" expected response body \"%s\" but got \"%s\"", req.RequestURI, expectedBody, body)
	}
}

func testH2ToH1ChunkedResponseServeH1(t *testing.T) *http.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "127.0.0.1:9443" {
			t.Errorf("r.Host doesn't match, %v!", r.Host)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !strings.HasPrefix(r.URL.Path, "/tov2ray") {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		defer r.Body.Close()
		bytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unable to read the response body %s", err)
		}

		n := len(bytes)

		var writer io.Writer
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			gw, err := gzip.NewWriterLevel(w, 5)
			if err != nil {
				t.Error("can't return gzip data")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer gw.Close()
			writer = gw
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Del("Content-Length")
			w.WriteHeader(200)
		} else {
			writer = w
		}
		if n > 0 {
			writer.Write(bytes[:])
		}
	})

	server := &http.Server{
		Addr:    "127.0.0.1:54321",
		Handler: handler,
	}
	return server
}

// GenerateRandomBytes returns securely generated random bytes.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

// GenerateRandomString returns a securely generated random string.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	bytes, err := GenerateRandomBytes(n)
	if err != nil {
		return "", err
	}
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes), nil
}

// startUpgradeBackend starts a backend that switches to an echo stream when it
// receives a WebSocket upgrade request, and answers other requests with
// "backend plain". It returns the address of the backend.
func startUpgradeBackend(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			// flushed so that the response is chunked and has no Content-Length,
			// which handlers that replace the response would not replace
			fmt.Fprint(w, "backend plain")
			_ = http.NewResponseController(w).Flush()
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
			return
		}
		_, _ = io.Copy(conn, brw.Reader)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// streamTestGlobals are the global options shared by the stream tests.
const streamTestGlobals = `{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
	grace_period  1ns
}
`

// upgradeClient is a client of an upgraded stream, speaking to the
// server over a plain TCP connection.
type upgradeClient struct {
	conn net.Conn
	br   *bufio.Reader
	resp *http.Response
}

// dialUpgrade sends an upgrade request for path to the HTTP port of the
// test server, and returns the client once the response headers are read.
func dialUpgrade(t *testing.T, path string, header map[string]string) *upgradeClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to dial the server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	req, err := http.NewRequest(http.MethodGet, "http://localhost:9080"+path, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	for k, v := range header {
		req.Header.Set(k, v)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := req.Write(conn); err != nil {
		t.Fatalf("failed to write upgrade request: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("failed to read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want %d", resp.StatusCode, http.StatusSwitchingProtocols)
	}
	return &upgradeClient{conn: conn, br: br, resp: resp}
}

// echo sends msg on the stream and expects to read it back.
func (c *upgradeClient) echo(t *testing.T, msg string) {
	t.Helper()
	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(c.conn, msg); err != nil {
		t.Fatalf("writing %d bytes to the stream: %v", len(msg), err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c.br, buf); err != nil {
		t.Fatalf("reading the echo of %d bytes: %v", len(msg), err)
	}
	if string(buf) != msg {
		t.Fatalf("echo does not match what was sent (%d bytes)", len(msg))
	}
}

// expectClosed waits for the server to close the stream.
func (c *upgradeClient) expectClosed(t *testing.T, within time.Duration) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(within))
	// the server may send a close message before it closes the connection
	_, err := io.Copy(io.Discard, c.br)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("the stream is still open after %s", within)
	}
}

// TestStreamDetachedSurvivesReload checks that, with stream_detached, an
// established stream keeps working across reloads that keep its upstream.
func TestStreamDetachedSurvivesReload(t *testing.T) {
	backend := startUpgradeBackend(t)
	tester := caddytest.NewTester(t)

	config := func(extra string) string {
		return streamTestGlobals + fmt.Sprintf(`
		localhost:9080 {
			%s
			reverse_proxy 127.0.0.1:%s {
				stream_detached
			}
		}
		`, extra, backend[strings.LastIndex(backend, ":")+1:])
	}

	tester.InitServer(config(""), "caddyfile")
	client := dialUpgrade(t, "/", nil)
	client.echo(t, "before the reload")

	for i, extra := range []string{`respond /one "one"`, `respond /two "two"`} {
		tester.InitServer(config(extra), "caddyfile")
		client.echo(t, fmt.Sprintf("after reload %d", i+1))
	}

	// the new config serves new streams as well
	second := dialUpgrade(t, "/", nil)
	second.echo(t, "new stream")
	client.echo(t, "the first stream is still there")
}

// TestStreamDetachedClosesRemovedUpstream checks that a reload that removes
// an upstream closes the detached streams of that upstream and only those.
func TestStreamDetachedClosesRemovedUpstream(t *testing.T) {
	port := func(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }
	backendA, backendB := startUpgradeBackend(t), startUpgradeBackend(t)
	tester := caddytest.NewTester(t)

	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
	localhost:9080 {
		handle /a* {
			reverse_proxy 127.0.0.1:%s {
				stream_detached
			}
		}
		handle /b* {
			reverse_proxy 127.0.0.1:%s {
				stream_detached
			}
		}
	}
	`, port(backendA), port(backendB)), "caddyfile")

	streamA, streamB := dialUpgrade(t, "/a", nil), dialUpgrade(t, "/b", nil)
	streamA.echo(t, "a before")
	streamB.echo(t, "b before")

	// the new config only has the second upstream
	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
	localhost:9080 {
		handle /b* {
			reverse_proxy 127.0.0.1:%s {
				stream_detached
			}
		}
		handle {
			respond "gone" 404
		}
	}
	`, port(backendB)), "caddyfile")

	streamA.expectClosed(t, 10*time.Second)
	streamB.echo(t, "b after")
	dialUpgrade(t, "/b", nil).echo(t, "b new")
}

// TestStreamDefaultClosedOnReload checks that, by default, a reload closes
// the upgraded streams: at once, or after stream_close_delay.
func TestStreamDefaultClosedOnReload(t *testing.T) {
	backend := startUpgradeBackend(t)
	port := backend[strings.LastIndex(backend, ":")+1:]
	tester := caddytest.NewTester(t)

	config := func(proxyOptions, extra string) string {
		return streamTestGlobals + fmt.Sprintf(`
		localhost:9080 {
			%s
			reverse_proxy 127.0.0.1:%s {
				%s
			}
		}
		`, extra, port, proxyOptions)
	}

	t.Run("immediately", func(t *testing.T) {
		tester.InitServer(config("", ""), "caddyfile")
		client := dialUpgrade(t, "/", nil)
		client.echo(t, "before the reload")

		tester.InitServer(config("", `respond /other "other"`), "caddyfile")
		client.expectClosed(t, 10*time.Second)
	})

	t.Run("after the close delay", func(t *testing.T) {
		const delay = 5 * time.Second
		tester.InitServer(config("stream_close_delay 5s", ""), "caddyfile")
		client := dialUpgrade(t, "/", nil)
		client.echo(t, "before the reload")

		tester.InitServer(config("stream_close_delay 5s", `respond /other "other"`), "caddyfile")
		reloaded := time.Now()
		// the stream is kept for the delay...
		client.echo(t, "after the reload")
		// ...and closed when it is over
		client.expectClosed(t, 4*delay)
		if elapsed := time.Since(reloaded); elapsed < delay/2 {
			t.Errorf("the stream was closed %s after the reload, long before the delay of %s", elapsed, delay)
		}
	})
}

// readAccessLog returns the entries of a JSON log file.
func readAccessLog(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("reading log: %v", err)
	}
	var entries []map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

// logEntries returns the entries of the log file with the given message
// and, if uri is not empty, for a request with that URI.
func logEntries(t *testing.T, path, msg, uri string) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, entry := range readAccessLog(t, path) {
		if entry["msg"] != msg {
			continue
		}
		if req, _ := entry["request"].(map[string]any); uri != "" && req["uri"] != uri {
			continue
		}
		found = append(found, entry)
	}
	return found
}

// waitForLogEntry waits for an entry to show up in the log file.
func waitForLogEntry(t *testing.T, path, msg, uri string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if found := logEntries(t, path, msg, uri); len(found) > 0 {
			return found[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no %q log entry (uri %q) after 10s; log: %v", msg, uri, readAccessLog(t, path))
	return nil
}

// TestDetachedStreamAccessLog checks the logging of detached streams: the
// handshake is logged unless skip_handshake is set, and the close of a stream
// is logged to the access log with the logger name, at the configured level
// (DEBUG by default).
func TestDetachedStreamAccessLog(t *testing.T) {
	backend := startUpgradeBackend(t)
	port := backend[strings.LastIndex(backend, ":")+1:]

	dir, err := os.MkdirTemp("", "caddy-stream-log")
	if err != nil {
		t.Fatalf("creating a log directory: %v", err)
	}
	// the log file may still be open when the test is over; do not fail on that
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	logFile := filepath.Join(dir, "access.log")

	tester := caddytest.NewTester(t)
	config := func(streamLogs string) string {
		return streamTestGlobals + fmt.Sprintf(`
		localhost:9080 {
			log access {
				output file %s
			}
			reverse_proxy 127.0.0.1:%s {
				stream_detached
				%s
			}
		}
		`, filepath.ToSlash(logFile), port, streamLogs)
	}

	// by default: the handshake is logged, the close of the stream is a DEBUG
	// entry that the access log (INFO) does not keep
	tester.InitServer(config(""), "caddyfile")
	first := dialUpgrade(t, "/first", nil)
	first.echo(t, "first stream")

	// the stream stays open when the config changes to skip the handshake
	// and to log the close of streams at INFO
	tester.InitServer(config(`stream_logs {
		skip_handshake
		level INFO
	}`), "caddyfile")
	first.echo(t, "first stream, after the reload")

	second := dialUpgrade(t, "/second", nil)
	second.echo(t, "second stream")
	second.conn.Close()

	closed := waitForLogEntry(t, logFile, "stream closed", "")
	if closed["logger"] != "http.log.access.access" {
		t.Errorf("close logged by %v, want http.log.access.access", closed["logger"])
	}
	if closed["result"] != "completed" {
		t.Errorf("close result = %v, want completed", closed["result"])
	}
	if closed["upstream"] != backend {
		t.Errorf("close upstream = %v, want %s", closed["upstream"], backend)
	}
	if n, _ := closed["bytes_to_backend"].(float64); n != float64(len("second stream")) {
		t.Errorf("bytes_to_backend = %v, want %d", closed["bytes_to_backend"], len("second stream"))
	}

	// the first stream, from the config that logs the handshake
	first.conn.Close()
	handled := waitForLogEntry(t, logFile, "handled request", "/first")
	if status, _ := handled["status"].(float64); status != http.StatusSwitchingProtocols {
		t.Errorf("handshake logged with status %v, want 101", handled["status"])
	}

	if got := logEntries(t, logFile, "handled request", "/second"); len(got) != 0 {
		t.Errorf("handshake of the second stream was logged despite skip_handshake: %v", got)
	}
	if got := logEntries(t, logFile, "stream closed", ""); len(got) != 1 {
		t.Errorf("%d stream close entries, want 1 (the first stream is logged at DEBUG): %v", len(got), got)
	}
}

// TestExtendedConnectH2 checks that a WebSocket over HTTP/2 (extended
// CONNECT, RFC 8441) is proxied to a backend that speaks HTTP/1.1 upgrades,
// by default and with detached streams.
func TestExtendedConnectH2(t *testing.T) {
	// The HTTP/2 server of the standard library only advertises extended
	// CONNECT when it is started with GODEBUG=http2xconnect=1, so the test runs
	// in a process of its own that has it, on other ports than the other tests.
	if os.Getenv(extendedConnectChildEnv) == "" {
		if testing.Short() {
			t.Skip("integration tests are skipped in short mode")
		}
		exe, err := os.Executable()
		if err != nil {
			t.Fatalf("cannot find the test binary: %v", err)
		}
		cmd := exec.Command(exe, "-test.run=^TestExtendedConnectH2$", "-test.count=1", "-test.v", "-test.timeout=3m")
		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1", extendedConnectChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			// leave out the logs of the server
			var report []string
			for _, line := range strings.Split(string(out), "\n") {
				if !strings.HasPrefix(line, `{"level"`) {
					report = append(report, line)
				}
			}
			t.Fatalf("extended CONNECT test failed: %v\n%s", err, strings.Join(report, "\n"))
		}
		return
	}

	backend := startUpgradeBackend(t)
	port := backend[strings.LastIndex(backend, ":")+1:]
	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: 2998})

	for name, proxyOptions := range map[string]string{"default": "", "detached": "stream_detached"} {
		t.Run(name, func(t *testing.T) {
			tester.InitServer(`{
				skip_install_trust
				admin localhost:2998
				http_port     9081
				https_port    9444
				grace_period  1ns
				servers :9444 {
					protocols h1 h2
				}
			}
			localhost {
				reverse_proxy 127.0.0.1:`+port+` {
					`+proxyOptions+`
				}
			}
			`, "caddyfile")

			client := dialH2(t, "127.0.0.1:9444")
			if !client.extendedConnect {
				t.Fatalf("the server does not advertise extended CONNECT (SETTINGS_ENABLE_CONNECT_PROTOCOL)")
			}
			client.openWebSocket(t, 1, "localhost:9444", "/ws")
			if client.status != "200" {
				t.Fatalf(":status = %q, want 200", client.status)
			}

			for _, msg := range []string{"hello over h2", "and again"} {
				client.send(t, 1, msg, false)
				if got := client.receive(t, len(msg)); got != msg {
					t.Fatalf("echo = %q, want %q", got, msg)
				}
			}

			// the client ends the stream, and the server follows
			client.send(t, 1, "", true)
			client.awaitEnd(t)
		})
	}
}

// h2Client is a minimal HTTP/2 client speaking frames, for a stream that the
// Go clients cannot open: a CONNECT with the :protocol pseudo-header.
type h2Client struct {
	conn            net.Conn
	fr              *http2.Framer
	dec             *hpack.Decoder
	extendedConnect bool // the server advertised support for extended CONNECT
	status          string
	data            []byte
	ended           bool
}

// extendedConnectChildEnv marks the process that runs the extended CONNECT test.
const extendedConnectChildEnv = "CADDY_TEST_EXTENDED_CONNECT_CHILD"

// dialH2 connects to the HTTPS address of the test server with HTTP/2,
// and exchanges the settings.
func dialH2(t *testing.T, addr string) *h2Client {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"h2"},
		ServerName:         "localhost",
	})
	if err != nil {
		t.Fatalf("failed to dial the server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if got := conn.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("negotiated protocol = %q, want h2", got)
	}

	c := &h2Client{conn: conn, fr: http2.NewFramer(conn, conn), dec: hpack.NewDecoder(4096, nil)}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("writing the preface: %v", err)
	}
	if err := c.fr.WriteSettings(); err != nil {
		t.Fatalf("writing settings: %v", err)
	}
	// the first frame of the server is its settings
	for !c.extendedConnect {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		f, err := c.fr.ReadFrame()
		if err != nil {
			t.Fatalf("reading the settings of the server: %v", err)
		}
		if sf, ok := f.(*http2.SettingsFrame); ok && !sf.IsAck() {
			_ = sf.ForeachSetting(func(s http2.Setting) error {
				// SETTINGS_ENABLE_CONNECT_PROTOCOL, RFC 8441
				if s.ID == http2.SettingID(0x8) && s.Val == 1 {
					c.extendedConnect = true
				}
				return nil
			})
			if err := c.fr.WriteSettingsAck(); err != nil {
				t.Fatalf("acknowledging settings: %v", err)
			}
			break
		}
	}
	return c
}

// openWebSocket sends the extended CONNECT request of a WebSocket
// on the stream, and reads the response headers.
func (c *h2Client) openWebSocket(t *testing.T, stream uint32, authority, path string) {
	t.Helper()
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, hf := range []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: authority},
		{Name: ":path", Value: path},
		{Name: "sec-websocket-version", Value: "13"},
	} {
		if err := enc.WriteField(hf); err != nil {
			t.Fatalf("encoding headers: %v", err)
		}
	}
	if err := c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatalf("writing headers: %v", err)
	}
	for c.status == "" {
		c.readFrame(t)
	}
}

// send writes data on the stream, and ends it if asked to.
func (c *h2Client) send(t *testing.T, stream uint32, data string, end bool) {
	t.Helper()
	if err := c.fr.WriteData(stream, end, []byte(data)); err != nil {
		t.Fatalf("writing data: %v", err)
	}
}

// receive reads n bytes of data from the stream.
func (c *h2Client) receive(t *testing.T, n int) string {
	t.Helper()
	for len(c.data) < n {
		c.readFrame(t)
	}
	got := string(c.data[:n])
	c.data = c.data[n:]
	return got
}

// awaitEnd waits for the server to end the stream.
func (c *h2Client) awaitEnd(t *testing.T) {
	t.Helper()
	for !c.ended {
		c.readFrame(t)
	}
}

// readFrame reads a frame of the server, and takes note of what is in it.
func (c *h2Client) readFrame(t *testing.T) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	f, err := c.fr.ReadFrame()
	if err != nil {
		t.Fatalf("reading a frame: %v", err)
	}
	switch f := f.(type) {
	case *http2.HeadersFrame:
		fields, err := c.dec.DecodeFull(f.HeaderBlockFragment())
		if err != nil {
			t.Fatalf("decoding headers: %v", err)
		}
		for _, field := range fields {
			if field.Name == ":status" {
				c.status = field.Value
			}
		}
		c.ended = c.ended || f.StreamEnded()
	case *http2.DataFrame:
		c.data = append(c.data, f.Data()...)
		c.ended = c.ended || f.StreamEnded()
	case *http2.RSTStreamFrame:
		// the server resets the stream when it is over, once the backend closed
		c.ended = true
	case *http2.PingFrame:
		if !f.IsAck() {
			_ = c.fr.WritePing(true, f.Data)
		}
	case *http2.GoAwayFrame:
		t.Fatalf("the server is going away: %v", f.ErrCode)
	}
}

// TestDetachedHTTP11Upgrade checks that HTTP/1.1 upgrades work with detached
// streams: the handshake response is the one of the backend, and a lot of data
// goes through in both directions on several streams.
func TestDetachedHTTP11Upgrade(t *testing.T) {
	backend := startUpgradeBackend(t)
	tester := caddytest.NewTester(t)
	tester.InitServer(streamTestGlobals+fmt.Sprintf(`
	localhost:9080 {
		reverse_proxy %s {
			stream_detached
		}
	}
	`, backend), "caddyfile")

	big := strings.Repeat("0123456789abcdef", 16*1024) // 256 KiB
	for i := range 3 {
		client := dialUpgrade(t, fmt.Sprintf("/stream%d", i), map[string]string{
			"Sec-WebSocket-Version": "13",
			"Sec-WebSocket-Key":     "dGhlIHNhbXBsZSBub25jZQ==",
		})
		if got := client.resp.Header.Get("Upgrade"); !strings.EqualFold(got, "websocket") {
			t.Errorf("Upgrade header = %q, want websocket", got)
		}
		if got := client.resp.Header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
			t.Errorf("Connection header = %q, want Upgrade", got)
		}
		client.echo(t, "small")
		client.echo(t, big)
	}

	// ordinary requests are proxied as usual
	tester.AssertGetResponse("http://localhost:9080/plain", 200, "backend plain")
}

// TestUpgradeThroughEncode checks that upgrades go through the encode
// handler, whether or not it handles the response.
func TestUpgradeThroughEncode(t *testing.T) {
	backend := startUpgradeBackend(t)
	tester := caddytest.NewTester(t)

	for name, proxyOptions := range map[string]string{"default": "", "detached": "stream_detached"} {
		t.Run(name, func(t *testing.T) {
			tester.InitServer(streamTestGlobals+fmt.Sprintf(`
			localhost:9080 {
				encode gzip
				reverse_proxy %s {
					%s
				}
			}
			`, backend, proxyOptions), "caddyfile")

			// without a key, encode wraps the response writer; with the key of a
			// browser it does not (encodings are not offered to WebSockets)
			for _, header := range []map[string]string{
				{"Accept-Encoding": "gzip"},
				{"Accept-Encoding": "gzip", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="},
			} {
				client := dialUpgrade(t, "/", header)
				if got := client.resp.Header.Get("Content-Encoding"); got != "" {
					t.Errorf("Content-Encoding = %q on an upgrade, want none", got)
				}
				client.echo(t, "through encode")
				client.echo(t, strings.Repeat("compressible ", 1000))
				client.conn.Close()
			}

			// other requests are still proxied through encode
			req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/plain", nil)
			req.Header.Set("Accept-Encoding", "gzip")
			tester.AssertResponseCode(req, http.StatusOK)
		})
	}
}
