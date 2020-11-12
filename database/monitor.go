package database

import (
	"cdc-distribute/conf"
	"cdc-distribute/database/postgres"
	"cdc-distribute/model"
	"errors"
)

type Monitor interface {
	Run(chan []*model.MessageWrapper) error
}

func Selector(database string, conf conf.Conf, rule model.QuickCheckRule) (m Monitor, err error) {
	switch database {
	case "postgres":
		return postgres.NewPostgresMonitor(conf, rule), nil
	default:
		return nil, errors.New("database offer not found")
	}
}
