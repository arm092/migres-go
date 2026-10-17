//go:build !windows

package app

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func (a *App) signalControl(ctx context.Context, out chan<- controlMessage) func() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-signals:
				req := Request{Command: "reset"}
				if sig == syscall.SIGUSR2 {
					parts := strings.Split(a.Config.Migration.CDC.ForcePosition, ":")
					if len(parts) != 2 {
						continue
					}
					n, err := strconv.ParseUint(parts[1], 10, 32)
					if err != nil {
						continue
					}
					req = Request{Command: "reposition", File: parts[0], Pos: uint32(n)}
				}
				select {
				case out <- controlMessage{req: req, reply: make(chan Response, 1)}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return func() { signal.Stop(signals) }
}
