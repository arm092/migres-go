package app

import (
	"context"

	"github.com/arm092/migres-go/internal/model"
)

func (a *App) recoverOperations(ctx context.Context) error {
	ops, err := a.Store.Operations()
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.State != "applying" {
			continue
		}
		req := Request{Command: op.Kind, File: op.Position.File, Pos: op.Position.Pos, GTID: op.Position.GTID, Discard: op.DiscardPending, AllowGap: op.AllowGap, Table: op.Table, ID: op.ID}
		if err = a.applyRequest(ctx, req); err != nil {
			return err
		}
		op.State = "done"
		if req.Command == "resnapshot" && req.Table != "" {
			op.State = "queued"
		}
		op.Error = ""
		if err = a.Store.Operation(op); err != nil {
			return err
		}
	}
	return nil
}
func pendingOperation(id, kind string, p model.Position) model.Operation {
	return model.Operation{ID: id, Kind: kind, State: "applying", Position: p}
}
