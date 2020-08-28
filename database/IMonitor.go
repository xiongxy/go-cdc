package database

import (
	"cdc-distribute/conf"
	"cdc-distribute/database/postgres"
	"errors"
	mapset "github.com/deckarep/golang-set"
)

type IMonitor interface {
	Monitor(exec func(mapset.Set))
}

func Selector(database string, identity int, listen conf.ListenModel, tableMap map[string]bool, quickTable map[string]mapset.Set) (IMonitor, error) {
	switch database {
	case "postgres":
		return postgres.NewPostgresMonitor(identity, listen, tableMap, quickTable), nil
	default:
		return nil, errors.New("database offer not found")
	}
}
