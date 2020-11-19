package conf

import (
	"context"
	"github.com/jackc/pgx/v4"
	"os"
)

func Load() []Conf {
	entities := loadDatabase()

	var res []Conf

	for _, v := range entities {
		configModel := Parse(v.Body)
		configModel.Identity = v.Id
		res = append(res, configModel)
	}
	return res

}

func loadDatabase() []cdcMonitorEntity {
	conn, _ := pgx.Connect(context.Background(), os.Getenv("postgres_db"))
	rows, _ := conn.Query(context.Background(), "SELECT * FROM cdc.monitor")

	var entities []cdcMonitorEntity

	for rows.Next() {
		entity := new(cdcMonitorEntity)
		_ = rows.Scan(&entity.Id, &entity.BodyType, &entity.Body)
		entities = append(entities, *entity)
	}
	return entities
}
