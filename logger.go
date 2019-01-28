package dynamodbcopy

import (
	"io"
	"log"
)

type Logger interface {
	Printf(format string, msg ...interface{})
}

type debugLogger struct {
	*log.Logger
	debug bool
}

func NewDebugLogger(writer io.Writer, prefix string, debug bool) Logger {
	logger := log.New(writer, prefix, log.Ltime)

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
