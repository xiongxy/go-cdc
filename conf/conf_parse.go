package conf

import (
	"cdc-distribute/log"
	"encoding/json"
)

func Parse(jsonStr string) Conf {
	var u Conf
	err := json.Unmarshal([]byte(jsonStr), &u)
	if err != nil {
		log.Logger.Warning("Parse configModel fail")
	}
	return u
}
