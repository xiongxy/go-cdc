package log

import (
	"github.com/sirupsen/logrus"
	"os"
)

// Logger is a shared logger
var Logger = logrus.New()

func init() {
	Logger.SetNoLock()
	Logger.Out = os.Stdout
	Logger.SetLevel(logrus.TraceLevel)
	Logger.SetFormatter(&logrus.JSONFormatter{})
}
