package process

import (
	"cdc-distribute/conf"
)

type Process interface {
	Write(wal ...*interface{}) error
	Close()
}

func NewProcess(conf conf.Conf) Process {
	switch {
	case conf.RabbitMqConf != nil:
		return newRabbitProcess(conf.RabbitMqConf)
	}
	return newFakeHandler(nil)
}
