package process

import (
	"cdc-distribute/conf"
	"cdc-distribute/log"
	"cdc-distribute/model"
	"encoding/json"
)

type fakeHandler struct{}

// newFakeHandler create a Handler print all data
func newFakeHandler(_ *conf.Conf) Process {
	return &fakeHandler{}
}

func (f *fakeHandler) Write(dataList ...*model.MessageWrapper) error {
	for _, data := range dataList {
		bytes, _ := json.Marshal(data)
		log.Logger.Printf("A message was sent to fakeHandler %v", string(bytes))
	}
	return nil
}

func (f *fakeHandler) Close() {}
