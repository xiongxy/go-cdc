package postgres

import (
	"github.com/deckarep/golang-set"
	"log"
)

func (m Monitor) PgOutputProcess(jsonData string) mapset.Set {
	ta := mapset.NewSet()
	log.Println("触发操作" + jsonData)
	return ta
}
