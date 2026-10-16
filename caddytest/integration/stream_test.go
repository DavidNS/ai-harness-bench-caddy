package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
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

// h2ExtendedConnectChildEnv marks the process that runs the test of
// extended CONNECT, see TestH2ExtendedConnectWebSocket.
const h2ExtendedConnectChildEnv = "CADDY_TEST_H2_EXTENDED_CONNECT"

// A WebSocket over HTTP/2 (RFC 8441 extended CONNECT) is proxied to an HTTP/1.1
// backend as an upgrade, and the data goes through both ways.
func TestH2ExtendedConnectWebSocket(t *testing.T) {
	// net/http only enables extended CONNECT in its HTTP/2 server when
	// GODEBUG=http2xconnect=1 is set when the process starts, so run the test
	// in a process started like that. It uses its own ports and admin
	// endpoint, since the servers of the tests which ran before this one may
	// be running in this process still.
	if os.Getenv(h2ExtendedConnectChildEnv) == "" {
		if testing.Short() {
			t.Skip("skipping integration test in short mode")
		}
		// not os.Args[0], which the tester replaces to start Caddy
		self, _ := os.Executable() // if it fails, so does running it
		cmd := exec.Command(self, "-test.run=^TestH2ExtendedConnectWebSocket$", "-test.count=1", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "GODEBUG=http2xconnect=1", h2ExtendedConnectChildEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("the test failed in the process with extended CONNECT enabled: %v\n%s", err, out)
		}
		return
	}

	const (
		adminPort = 2998
		httpsPort = 9445
	)

	startUpgradeBackend(t, streamBackendA)

	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: adminPort})
	tester.InitServer(fmt.Sprintf(`{
		skip_install_trust
		admin localhost:%d
		http_port     9081
		https_port    %d
		auto_https    disable_redirects
		grace_period  1ns
	}

	localhost:%d {
		reverse_proxy %s
	}
	`, adminPort, httpsPort, httpsPort, streamBackendA), "caddyfile")

	// The standard clients can't send the :protocol pseudo-header, so speak
	// HTTP/2 by hand.
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", httpsPort), &tls.Config{ //nolint:gosec
		InsecureSkipVerify: true,
		ServerName:         "localhost",
		NextProtos:         []string{"h2"},
	})
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer conn.Close()
	// don't hang if the data doesn't come back
	conn.SetDeadline(time.Now().Add(streamTestWait))

	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	// wait for the settings of the server, which has to allow extended CONNECT
	const settingEnableConnectProtocol = http2.SettingID(0x8)
	var extendedConnect bool
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("reading the settings of the server: %v", err)
		}
		sf, ok := f.(*http2.SettingsFrame)
		if !ok || sf.IsAck() {
			continue
		}
		if v, ok := sf.Value(settingEnableConnectProtocol); ok && v == 1 {
			extendedConnect = true
		}
		if err := fr.WriteSettingsAck(); err != nil {
			t.Fatal(err)
		}
		break
	}
	if !extendedConnect {
		t.Fatal("the HTTP/2 server doesn't allow extended CONNECT")
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range [][2]string{
		{":method", "CONNECT"},
		{":scheme", "https"},
		{":authority", fmt.Sprintf("localhost:%d", httpsPort)},
		{":path", "/ws"},
		{":protocol", "websocket"},
	} {
		if err := enc.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}); err != nil {
			t.Fatal(err)
		}
	}
	const streamID = 1
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}

	// readStream returns the next n bytes of data of the stream; the
	// response headers are checked on the way
	var (
		dec      = hpack.NewDecoder(4096, nil)
		pending  []byte
		gotHead  bool
		readData = func(n int) string {
			for len(pending) < n {
				f, err := fr.ReadFrame()
				if err != nil {
					t.Fatalf("reading the stream: %v", err)
				}
				switch f := f.(type) {
				case *http2.HeadersFrame:
					fields, err := dec.DecodeFull(f.HeaderBlockFragment())
					if err != nil {
						t.Fatalf("decoding the response headers: %v", err)
					}
					for _, field := range fields {
						if field.Name == ":status" && field.Value != "200" {
							t.Fatalf("status = %s, want 200", field.Value)
						}
					}
					gotHead = true
				case *http2.DataFrame:
					pending = append(pending, f.Data()...)
				case *http2.PingFrame:
					if !f.IsAck() {
						fr.WritePing(true, f.Data)
					}
				case *http2.RSTStreamFrame:
					t.Fatalf("the stream was reset: %v", f.ErrCode)
				case *http2.GoAwayFrame:
					t.Fatalf("the connection was closed: %v", f.ErrCode)
				}
			}
			got := string(pending[:n])
			pending = pending[n:]
			return got
		}
	)

	for _, msg := range []string{"hello over h2", "and again"} {
		if err := fr.WriteData(streamID, false, []byte(msg)); err != nil {
			t.Fatalf("writing %q: %v", msg, err)
		}
		if got := readData(len(msg)); got != msg {
			t.Fatalf("echo = %q, want %q", got, msg)
		}
	}
	if !gotHead {
		t.Error("the data came without response headers")
	}
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
