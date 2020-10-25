package kernel

import (
	golog "log"
)

type Logger struct {
	Enable bool
}

func (l *Logger) Println(args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Println(args...)
}

func (l *Logger) Printf(template string, args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Printf(template, args...)
}

func (l *Logger) Panic(args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Panic(args...)
}
func (l *Logger) Panicln(args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Panicln(args...)
}
func (l *Logger) Panicf(template string, args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Panicf(template, args...)
}
func (l *Logger) Fatal(args ...interface{}) {
	if !l.Enable {
		return
	}
	golog.Fatal(args...)
}
