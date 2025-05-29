package mapreducese

import (
	"errors"
	"fmt"
	"go-mr/types"
	"io"
	"log"
	"plugin"
)

// MapReduceSequential executes a MapReduce workflow sequentially.
type MapReduceSequential struct {
	Mapper  types.Mapper
	Reducer types.Reducer
}

// LoadMapper loads a Mapper plugin from the given path.
func (mr *MapReduceSequential) LoadMapper(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}
	sym, err := p.Lookup("Map")
	if err != nil {
		return err
	}
	// rawFunc has the right signature, convert it to your named Mapper type
	rawFunc, ok := sym.(func(string) []types.KeyValue)
	if !ok {
		return fmt.Errorf("invalid mapper signature: %T", sym)
	}
	mr.Mapper = types.Mapper(rawFunc)
	return nil
}

func (mr *MapReduceSequential) LoadReducer(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}
	sym, err := p.Lookup("Reduce")
	if err != nil {
		return err
	}
	rawFunc, ok := sym.(func(string, []string) string)
	if !ok {
		return fmt.Errorf("invalid reducer signature: %T", sym)
	}
	mr.Reducer = types.Reducer(rawFunc)
	return nil
}

// Run reads data from the provided reader, maps, groups inline, and reduces it.
func (mr *MapReduceSequential) Run(r io.Reader) (map[string]string, error) {
	if mr.Mapper == nil {
		return nil, errors.New("no mapper loaded")
	}
	if mr.Reducer == nil {
		return nil, errors.New("no reducer loaded")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	log.Printf("Mapping input of %d bytes", len(data))
	kvs := mr.Mapper(string(data))

	// Inline grouping logic
	grouped := make(map[string][]string)
	for _, kv := range kvs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}
	log.Printf("Reducing %d keys", len(grouped))

	output := make(map[string]string, len(grouped))
	for key, values := range grouped {
		output[key] = mr.Reducer(key, values)
	}
	return output, nil
}
