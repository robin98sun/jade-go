package kernel

import (
	golog "log"
)

type Logger struct {
	Debug *Printer
	Perf *Printer
	Op    *Printer
}

func NewLogger() *Logger {
	return &Logger{
		Debug: &Printer{
			Enabled: false,
		},
		Perf: &Printer{
			Enabled: false,
		},
		Op: &Printer{
			Enabled: false,
		},
	}
}

type Printer struct {
	Enabled bool
}

func (p *Printer) Println(args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Println(args...)
}

func (p *Printer) Printf(template string, args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Printf(template, args...)
}

func (p *Printer) Panic(args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Panic(args...)
}
func (p *Printer) Panicln(args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Panicln(args...)
}
func (p *Printer) Panicf(template string, args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Panicf(template, args...)
}
func (p *Printer) Fatal(args ...interface{}) {
	if !p.Enabled {
		return
	}
	golog.Fatal(args...)
}
