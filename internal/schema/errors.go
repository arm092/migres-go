package schema

import "errors"

var ErrSnapshotInvalidated = errors.New("snapshot invalidated by source DDL")
