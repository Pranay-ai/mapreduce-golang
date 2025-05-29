package master

type WorkerInfo struct {
	// Add fields as needed for the worker node
	ID             string // Unique identifier for the worker
	Address        string // Address of the worker node
	Port           int    // Port number for the worker node
	Active         bool   // Indicates if the worker is currently active
	TasksCompleted int    // Number of tasks completed by the worker
}

type MasterNode struct {

	// Add fields as needed for the master node
	workers        map[string]*WorkerInfo
	inputfilepath  string
	pluginfilepath string
	outputfilepath string
}
