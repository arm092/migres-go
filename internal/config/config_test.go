package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStrictConfigAndCDCEnvironment(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte("mysql:\n  database: source\nclickhouse:\n  database: target\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CDC_SERVER_ID", "1234")
	t.Setenv("CDC_PRODUCER_FLUSH_INTERVAL", "250ms")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Migration.CDC.ServerID != 1234 || c.Migration.CDC.ProducerFlush != 250*time.Millisecond {
		t.Fatal("CDC overrides ignored")
	}
	t.Setenv("CDC_SERVER_ID", "broken")
	if _, err = Load(p); err == nil {
		t.Fatal("invalid environment accepted")
	}
}
func TestUnknownConfigFails(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yml")
	os.WriteFile(p, []byte("typo: true\n"), 0600)
	if _, err := Load(p); err == nil {
		t.Fatal("unknown key accepted")
	}
}

func TestDebugRetentionWholeSeconds(t *testing.T) {
	for _, retention := range []time.Duration{0, -time.Second, 500 * time.Millisecond, 1500 * time.Millisecond, time.Second, 2 * time.Second, 24 * time.Hour} {
		t.Run(retention.String(), func(t *testing.T) {
			c := Default()
			c.MySQL.Database = "source"
			c.ClickHouse.Database = "target"
			c.DebugRetention = retention
			err := c.Validate()
			valid := retention >= time.Second && retention%time.Second == 0
			if valid && err != nil {
				t.Fatal(err)
			}
			if !valid && (err == nil || !strings.Contains(err.Error(), "debug_retention")) {
				t.Fatalf("expected retention validation error, got %v", err)
			}
		})
	}
}
