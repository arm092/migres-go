//go:build windows

package app

import "context"

func (a *App) signalControl(ctx context.Context, out chan<- controlMessage) func() { return func() {} }
