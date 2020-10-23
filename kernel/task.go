package kernel

import (
// "math/rand"
// "strconv"
// "time"
)

type Task struct {
	Application  *Application        `json:"application,omitempty"`
	Requirements *Requirements       `json:"requirements,omitempty"`
	Budget       *Budget             `json:"budget,omitempty"`
	Key          string              `json:"key,omitempty"`
	SubtaskKey   string              `json:"subtaskKey,omitempty"`
	Subtasks     map[string]*SubTask `json:"subtasks,omitempty"`
}

func (t *Task) GetKey() string {
	if t.Key == "" {
		t.Key = t.Application.Key() + ":" + RandomString()
	}
	return t.Key
}

// func (t *Task) NewSubtask(item interface{}) string {
// 	if len(t.Subtasks) == 0 {
// 		t.Subtasks = make(map[string]interface{})
// 	}
// 	subtaskKey := t.GetKey() + ":" + RandomString()
// 	for _, exists := t.Subtasks[subtaskKey]; exists; _, exists = t.Subtasks[subtaskKey] {
// 		subtaskKey = t.GetKey() + ":" + RandomString()
// 	}
// 	t.Subtasks[subtaskKey] = item
// 	return subtaskKey
// }

// func (t *Task) RemoveSubtask(key string) interface{} {
// 	if item, exists := t.Subtasks[key]; exists {
// 		delete(t.Subtasks, key)
// 		return item
// 	}
// 	return nil
// }

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
