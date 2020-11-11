package database

import (
	"cdc-distribute/conf"
	"cdc-distribute/database/postgres"
	"errors"
	mapSet "github.com/deckarep/golang-set"
)

type Monitor interface {
	Run(exec func(mapSet.Set)) error
}

func Selector(database string, conf conf.Conf, tableColumn map[string][]string, tableMap map[string]bool, quickTable map[string]mapSet.Set) (m Monitor, err error) {
	switch database {
	case "postgres":
		return postgres.NewPostgresMonitor(conf, tableColumn, tableMap, quickTable), nil
	default:
		return nil, errors.New("database offer not found")
	}
}
