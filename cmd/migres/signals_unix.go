//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func registerTermination(cancel func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	go func() { <-ch; cancel() }()
}
