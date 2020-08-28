package conf

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
)

func Parse(jsonStr string) ConfigModel {
	var u ConfigModel
	err := json.Unmarshal([]byte(jsonStr), &u)
	if err != nil {
		logrus.Warning("Parse configModel fail")
	}
	return u
}
