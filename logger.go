package dynamodbcopy

import (
	"log"
	"os"
)

type Logger interface {
	Printf(format string, msg ...interface{})
}

type debugLogger struct {
	*log.Logger
	debug bool
}

func NewDebugLogger(prefix string, debug bool) Logger {
	logger := log.New(os.Stdout, prefix, log.Ltime)

	return debugLogger{
		Logger: logger,
		debug:  debug,
	}
}

func (l debugLogger) Printf(format string, msg ...interface{}) {
	if !l.debug {
		return
	}

	l.Logger.Printf(format, msg...)
}
