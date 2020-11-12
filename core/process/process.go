package process

import (
	"cdc-distribute/conf"
	"cdc-distribute/model"
)

type Process interface {
	Write(wal ...*model.MessageWrapper) error
	Close()
}

func NewProcess(conf conf.Conf) Process {
	switch {
	case conf.RabbitMqConf != nil:
		return newRabbitProcess(conf.RabbitMqConf)
	}
	return newFakeHandler(nil)
}
