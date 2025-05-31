package worker

import (
	"crypto/rand"
	"fmt"
	"plugin"
)

func NewWorkerNode(address, port, masterAddress, masterPort string) (*WorkerNode, error) {
	id, err := GenerateUniqueID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate unique ID: %v", err)
	}

	return &WorkerNode{
		ID:      id,
		Address: address,
		Port:    port,
		Active:  true, // Workers are active by default
		MasterNode: &MasterNode{
			Address: masterAddress,
			Port:    masterPort,
		},
		Mapper:  nil, // Mapper function will be set later
		Reducer: nil, // Reducer function will be set later
	}, nil
}

func (w *WorkerNode) Map(inputFile string, outputFile string, pluginFile string) error {

	w.LoadMapper(pluginFile)
	if w.Mapper == nil {
		return fmt.Errorf("no mapper function loaded")
	}

	fmt.Printf("Worker %s is processing map task on file %s\n", w.ID, inputFile)

	return nil
}
func (w *WorkerNode) Reduce(inputFiles []string, outputFile string, pluginFile string) error {

	fmt.Printf("Worker %s is processing reduce task on files %v\n", w.ID, inputFiles)

	return nil
}

func GenerateUniqueID() (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 5

	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b), nil
}

func (w *WorkerNode) LoadMapper(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}
	sym, err := p.Lookup("Map")
	if err != nil {
		return err
	}

	rawFunc, ok := sym.(func(string) []KeyValue)
	if !ok {
		return fmt.Errorf("invalid mapper signature: %T", sym)
	}
	w.Mapper = Mapper(rawFunc)
	return nil
}

func (w *WorkerNode) LoadReducer(path string) error {
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
	w.Reducer = Reducer(rawFunc)
	return nil
}
