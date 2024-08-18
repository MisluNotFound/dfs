package temp

import (
	"strconv"
	"strings"
)

type tempInfo struct {
	Uuid string
	Hash string
	Size int64
}

func (t *tempInfo) hash() string {
	s := strings.Split(t.Hash, ".")
	return s[0]
}

func (t *tempInfo) id() int {
	s := strings.Split(t.Hash, ".")
	id, _ := strconv.Atoi(s[1])
	return id
}
