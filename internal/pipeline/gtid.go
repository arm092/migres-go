package pipeline

import gm "github.com/go-mysql-org/go-mysql/mysql"

func MergeGTID(base string, delta gm.GTIDSet) (string, error) {
	if delta == nil {
		return base, nil
	}
	if base == "" {
		return delta.String(), nil
	}
	set, err := gm.ParseGTIDSet("mysql", base)
	if err != nil {
		return "", err
	}
	if err = set.Update(delta.String()); err != nil {
		return "", err
	}
	return set.String(), nil
}
