package main

import (
	"go-mr/types"
	"strconv"
	"strings"
)

func Map(record string) []types.KeyValue {
	words := strings.Fields(record)
	kvs := make([]types.KeyValue, len(words))
	for i, word := range words {
		kvs[i] = types.KeyValue{Key: word, Value: "1"}
	}
	return kvs
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
