package kernel

import (
// "math/rand"
// "strconv"
// "time"
)

type Task struct {
	Application  *Application  `json:"application,omitempty"`
	Requirements *Requirements `json:"requirements,omitempty"`
	Budget       *Budget       `json:"budget,omitempty"`
	Key          string        `json:"key,omitempty"`
}

func (t *Task) GenKey() {
	if t.Key != "" {
		return
	}
	t.Key = t.Application.Key() + ":" + RandomString()
}

func (t *Task) Valid() bool {
	if !t.Application.valid() {
		return false
	}
	if !t.Requirements.valid() {
		return false
	}
	if !t.Budget.valid() {
		return false
	}
	return true
}

// Budget the budget specification
type Budget struct {
	MaximumMilliseconds int `json:"maximumMilliseconds,omitempty"`
	Price               int `json:"price,omitempty"`
}

func (b *Budget) valid() bool {
	if b.MaximumMilliseconds == 0 || b.Price == 0 {
		return false
	}
	return true
}
