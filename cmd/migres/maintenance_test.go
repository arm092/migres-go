package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/journal"
)

func TestOfflineMaintenanceCLI(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(config, []byte("mysql:\n  database: unavailable_source\nclickhouse:\n  database: unavailable_target\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATA_DIR", dir)
	s, err := journal.Open(filepath.Join(dir, "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetMeta("instance_id", "cli-owner"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	original := os.Args
	defer func() { os.Args = original }()
	backup := filepath.Join(dir, "backup.db")
	for _, command := range []string{"backup", "compact"} {
		os.Args = []string{"migres", command, "--config", config, "--output", backup + command}
		if err = run(); err != nil {
			t.Fatal(command, err)
		}
	}
	t.Setenv("DATA_DIR", filepath.Join(dir, "restored"))
	os.Args = []string{"migres", "restore", "--config", config, "--input", backup + "backup"}
	if err = run(); err != nil {
		t.Fatal(err)
	}
	if err = run(); err == nil {
		t.Fatal("overwrote existing restored journal")
	}
	r, err := journal.Open(filepath.Join(dir, "restored", "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var owner string
	if err = r.Meta("instance_id", &owner); err != nil || owner != "cli-owner" {
		t.Fatal(owner, err)
	}
}
