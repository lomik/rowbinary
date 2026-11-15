// Package chtest manages ClickHouse instances for repository tests, not library clients.
// It lives in the integration module so the library module stays free of Docker dependencies.
package chtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image pins the multi-architecture 25.9 patch release, including its manifest digest.
const Image = "clickhouse/clickhouse-server:25.9.7.56@sha256:67593ce357b801a875e1200546e02c5ec360eec15185a5ec93a6cbdb0719de40"

const username = "rowbinary"
const password = "rowbinary-test-password"

type Server struct {
	DSN, NativeAddr, Username, Password string
}

// Config deliberately defaults to offline. A DSN alone cannot accidentally enable integration.
func Config(getenv func(string) string) (bool, Server, error) {
	mode := getenv("ROWBINARY_TEST_MODE")
	if mode == "" || mode == "offline" {
		return false, Server{}, nil
	}
	if mode != "integration" {
		return false, Server{}, fmt.Errorf("ROWBINARY_TEST_MODE must be offline or integration, got %q", mode)
	}
	s := Server{DSN: getenv("ROWBINARY_TEST_DSN"), NativeAddr: getenv("ROWBINARY_TEST_NATIVE_ADDR")}
	if s.DSN != "" {
		u, err := url.Parse(s.DSN)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || (u.Path != "" && u.Path != "/") || u.Fragment != "" {
			return false, Server{}, errors.New("ROWBINARY_TEST_DSN must be an HTTP(S) server URL with optional credentials, without path/query/fragment")
		}
		s.Username = u.User.Username()
		s.Password, _ = u.User.Password()
	}
	return true, s, nil
}

// Ready verifies a real authenticated query, not merely an open TCP port.
func Ready(ctx context.Context, dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("query", "SELECT 1")
	u.RawQuery = q.Encode()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && readErr == nil && strings.TrimSpace(string(body)) == "1" {
				return nil
			}
			err = fmt.Errorf("query returned HTTP %d (read error: %v)", resp.StatusCode, readErr)
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("ClickHouse authenticated readiness: %w (last attempt: %v)", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Start owns only its new container; an external DSN is checked but never stopped.
func Start(ctx context.Context, external Server) (server Server, cleanup func() error, err error) {
	cleanup = func() error { return nil }
	// Some Docker discovery paths in testcontainers panic instead of returning errors.
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("ClickHouse testcontainers startup: %v (is Docker running?)", p)
		}
	}()
	if external.DSN != "" {
		return external, cleanup, Ready(ctx, external.DSN)
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        Image,
			ExposedPorts: []string{"8123/tcp", "9000/tcp"},
			Env:          map[string]string{"CLICKHOUSE_USER": username, "CLICKHOUSE_PASSWORD": password, "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"},
			HostConfigModifier: func(h *container.HostConfig) {
				h.PortBindings = nat.PortMap{"8123/tcp": {{HostIP: "127.0.0.1"}}, "9000/tcp": {{HostIP: "127.0.0.1"}}}
			},
			WaitingFor: wait.ForListeningPort("8123/tcp").WithStartupTimeout(90 * time.Second),
		}, Started: true,
	})
	if c != nil {
		cleanup = func() error {
			stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return c.Terminate(stopCtx)
		}
	}
	if err != nil {
		return server, cleanup, fmt.Errorf("start ClickHouse container (Docker required): %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return server, cleanup, err
	}
	httpPort, err := c.MappedPort(ctx, "8123/tcp")
	if err != nil {
		return server, cleanup, err
	}
	nativePort, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return server, cleanup, err
	}
	u := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, httpPort.Port()), User: url.UserPassword(username, password)}
	server = Server{DSN: u.String(), NativeAddr: net.JoinHostPort(host, nativePort.Port()), Username: username, Password: password}
	return server, cleanup, Ready(ctx, server.DSN)
}

// Main starts at most one container per test binary and always reports teardown errors.
func Main(m *testing.M, configure func(Server)) int {
	return run(m.Run, configure, os.Getenv, Start)
}

func run(tests func() int, configure func(Server), getenv func(string) string, start func(context.Context, Server) (Server, func() error, error)) int {
	integration, external, err := Config(getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if !integration {
		return tests()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	s, cleanup, err := start(ctx, external)
	cancel()
	code := 1
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration requested:", err)
	} else {
		configure(s)
		code = tests()
	}
	if err := cleanup(); err != nil {
		fmt.Fprintln(os.Stderr, "ClickHouse cleanup:", err)
		code = 1
	}
	return code
}
