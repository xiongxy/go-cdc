package main

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/runner"
	"cdc-distribute/log"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"syscall"
)

func init() {
	environment := os.Getenv("GO_ENVIRONMENT")
	if "" == environment {
		_ = godotenv.Load()
	}
}

func main() {

	models := conf.Load()

	for _, conf := range models {
		runner, err := runner.New(conf).Builder()
		if err != nil {
			log.Logger.WithFields(logrus.Fields{
				"err":    err,
				"config": conf,
			}).Error("create runner err")
		} else {
			go runner.Run()
			log.Logger.Printf("run monitor task , the number is : %conf", conf.Identity)
		}
	}

	prometheusAddress := os.Getenv("prometheus_address")

	if prometheusAddress != "" {
		log.Logger.Info("start prometheus handler")
		// prometheus exporter
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(prometheusAddress, nil)
	}

	log.Logger.Info("start go-cdc...")

	c := make(chan os.Signal)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				log.Logger.Println("退出", s)
				os.Exit(0)
			default:
				log.Logger.Println("other", s)
			}
		}
	}()

	// block forever
	select {}
}
