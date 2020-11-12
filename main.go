package main

import (
	"cdc-distribute/conf"
	"cdc-distribute/core/process"
	"cdc-distribute/core/runner"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
)

func init() {
	godotenv.Load()
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.TraceLevel)
}

func main() {

	models := conf.Load()

	go process.LoopProcess()

	for _, v := range models {
		runner, err := runner.New(v).Builder()
		if err != nil {
			logrus.Errorf("create replication slot err: %v", err)
		} else {
			go runner.Run()
		}
	}

	prometheusAddress := os.Getenv("prometheus_address")

	if prometheusAddress != "" {
		logrus.Logger.Info("start prometheus handler")
		// prometheus exporter
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(prometheusAddress, nil)
	}

	logrus.Logger.Info("start go-cdc...")

	// block forever
	select {}
}
