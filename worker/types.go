package worker

import "errors"

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

type WorkerNode struct {
	ID         string      // Unique identifier for the worker
	Address    string      // Address of the worker node
	Port       string      // Port number for the worker node
	Active     bool        // Indicates if the worker is currently active
	MasterNode *MasterNode // Reference to the master node this worker is connected to
	Mapper     Mapper      // Function to perform map tasks
	Reducer    Reducer     // Function to perform reduce tasks
}

type MasterNode struct {
	Address string // Address of the master node
	Port    string // Port number for the master node

}
