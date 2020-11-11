package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(successCounterVec)
	prometheus.MustRegister(errorCounterVec)
}

// successCounterVec monitor success count
var successCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "go_cdc_counter_success",
	Help: "success count",
}, []string{"slot_name"})

// errorCounterVec monitor error count
var errorCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "go_cdc_counter_error",
	Help: "error count",
}, []string{"slot_name"})

// IncreaseSuccessCount increase success count for slotName
func IncreaseSuccessCount(slotName string, count int) {
	successCounterVec.WithLabelValues(slotName).Add(float64(count))
}

// IncreaseErrorCount increase error count for slotName
func IncreaseErrorCount(slotName string, count int) {
	errorCounterVec.WithLabelValues(slotName).Add(float64(count))
}
