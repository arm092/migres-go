package app

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/config"
)

func TestStartupStatus(t *testing.T) {
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("isolated fixture required")
	}
	c := config.Default()
	c.MySQL.Port = 33080
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_deliberately_absent"
	c.ClickHouse.Port = 39000
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	c.RuntimeDir = filepath.Join(c.DataDir, "run")
	c.Listen = "127.0.0.1:0"
	if host := os.Getenv("MIGRES_TEST_HOST"); host != "" {
		c.MySQL.Host = host
		c.ClickHouse.Host = host
	}
	a, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for {
		out, e := Control(ctx, c, Request{Command: "status"})
		if e == nil && strings.Contains(out.Diagnostic, "Unknown database") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("startup failure was not reported")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if a.Ready.Load() || !a.Blocked.Load() {
		t.Fatal("invalid startup readiness state")
	}
	r := httptest.NewRecorder()
	a.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/livez", nil))
	if r.Code != 200 {
		t.Fatal("database failure broke process liveness")
	}
}
