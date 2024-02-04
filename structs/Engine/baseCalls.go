package Engine

import (
	"strconv"
	"strings"
	"time"
)

func (e *Engine) put(call string) {
	parts := strings.Split(call, " ")
	key := parts[1]
	value := []byte(strings.Join(parts[2:], " "))

	err := e.writePath(key, value, false)
	if err != nil {
		return
	}

}

func (e *Engine) delete(call string) {
	parts := strings.Split(call, " ")
	key := parts[1]

	rec, err := e.readPath(key)
	if err != nil {
		return
	}
	if rec != nil {
		err := e.writePath(rec.GetKey(), rec.GetValue(), true)
		if err != nil {
			return
		}
	}
}

func (e *Engine) get(call string) {
	parts := strings.Split(call, " ")
	key := parts[1]

	rec, err := e.readPath(key)
	if err != nil {
		return
	}
	if rec != nil {
		rec.PrintRecord()
	}
}

func (e *Engine) quit() {
	return
}

func (e *Engine) logToken(tokenBytes []byte) {
	key := "tokenLog " + strconv.FormatInt(time.Now().Unix(), 10)
	e.writePath(key, tokenBytes, false)
}
