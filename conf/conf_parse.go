package conf

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
)

func Parse(jsonStr string) Conf {
	var u Conf
	err := json.Unmarshal([]byte(jsonStr), &u)
	if err != nil {
		logrus.Warning("Parse configModel fail")
	}
	return u
}
