package types

import (
	"errors"
)

type KeyValue struct {
	Key   string
	Value string
}

// NewKeyValue creates a new KeyValue instance.
type Mapper func(record string) []KeyValue

// Reducer is a function type that takes a key and a slice of values,
type Reducer func(key string, values []string) string

var ErrInvalidMapper = errors.New("invalid mapper: plugin symbol Map does not implement Mapper interface")

var ErrInvalidReducer = errors.New("invalid reducer: plugin symbol Reduce does not implement Reducer interface")
