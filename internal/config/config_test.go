package config

import (
	"os"
	"path/filepath"
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
