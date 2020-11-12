package process

import (
	"cdc-distribute/conf"
	"cdc-distribute/model"
	"encoding/json"
	"github.com/sirupsen/logrus"
)

type fakeHandler struct{}

// newFakeHandler create a Handler print all data
func newFakeHandler(_ *conf.Conf) Process {
	return &fakeHandler{}
}

func (l *fakeHandler) Write(datas ...*model.MessageWrapper) error {
	for _, data := range datas {
		bytes, _ := json.Marshal(data)
		logrus.Infof(string(bytes))
	}
	return nil
}

func (l *fakeHandler) Close() {}
