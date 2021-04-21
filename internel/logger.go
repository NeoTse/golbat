package internel

import (
	"log"
	"os"
)

type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

type level int

const (
	DEBUG level = iota
	INFO
	WARNING
	ERROR
)

type defaultLog struct {
	*log.Logger
	level level
}

func defaultLogger(level level) *defaultLog {
	return &defaultLog{
		Logger: log.New(os.Stderr, "golbat ", log.LstdFlags),
		level:  level,
	}
}

func (l *defaultLog) Debugf(format string, v ...interface{}) {
	if l.level <= DEBUG {
		l.Printf("DEBUG: "+format, v...)
	}
}

func (l *defaultLog) InfoF(format string, v ...interface{}) {
	if l.level <= INFO {
		l.Printf("INFO: "+format, v...)
	}
}

func (l *defaultLog) Warningf(format string, v ...interface{}) {
	if l.level <= WARNING {
		l.Printf("WARNING: "+format, v...)
	}
}

func (l *defaultLog) Errorf(format string, v ...interface{}) {
	if l.level <= ERROR {
		l.Printf("ERROR: "+format, v...)
	}
}
