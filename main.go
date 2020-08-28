package main

import (
	"cdc-distribute/conf"
	"cdc-distribute/core"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	godotenv.Load()
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)
}

func main() {
	models := conf.Load()
	for _, v := range models {
		go core.NewRunner(v).Builder().Run()
	}
}
