package query

import (
	"fmt"
)

type QueryData struct {
	Message string
	sth string
}

func (q *QueryData) Query() {
	q.Check()
	fmt.Println("this is query interface:", q.Print())
}

func (q *QueryData) Check() {
	if q.Message != "" {
		q.sth = q.Message + ", checked"
	}
}

func (q *QueryData) Print() string {
	return q.sth
}