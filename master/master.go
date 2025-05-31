package master

import (
	"fmt"
	"os"
	"path/filepath"
)

type WorkerInfo struct {
	ID      string // Unique identifier for the worker
	Address string // Address of the worker node
	Port    string // Port number for the worker node
	Active  bool   // Indicates if the worker is currently active
}

type ExecutionPhase int

const (
	PhaseMap ExecutionPhase = iota
	PhaseIdle
	PhaseReduce
	PhaseDone
)

type TaskRequest struct {
	WorkerID string
	ReplyCh  chan *TaskResponse
}

type TaskResponse struct {
	TaskID    string
	TaskType  string // e.g., "map" or "reduce"
	InputPath string
	Metadata  map[string]string
}

type TaskStatusReport struct {
	WorkerID          string
	TaskID            string
	Success           bool
	Error             string
	IntermediateFiles map[string]string // reducerID -> file path
}

type MasterNode struct {
	workers                  map[string]*WorkerInfo
	numberReducers           int // Number of reducers to use
	inputfilepath            string
	pluginfilepath           string
	outputfilepath           string
	requestChannel           chan *TaskRequest
	taskSubmissionChannel    chan *TaskStatusReport // Channel for task submissions
	phase                    ExecutionPhase
	pendingTasks             []*TaskResponse
	activeTasks              map[string]string   // taskID -> workerID
	workerIdTaskMap          map[string][]string // workerID -> list of taskIDs
	reducerIntermediateFiles map[string][]string // reducerID -> intermediate file paths
}

func NewMasterNode(inputFile, pluginFile, outputFile string, numberReducers int) *MasterNode {
	return &MasterNode{
		workers:                  make(map[string]*WorkerInfo),
		inputfilepath:            inputFile,
		pluginfilepath:           pluginFile,
		outputfilepath:           outputFile,
		requestChannel:           make(chan *TaskRequest),
		taskSubmissionChannel:    make(chan *TaskStatusReport),
		workerIdTaskMap:          make(map[string][]string),
		numberReducers:           numberReducers,
		reducerIntermediateFiles: make(map[string][]string),
		pendingTasks:             make([]*TaskResponse, 0),
		activeTasks:              make(map[string]string),
		phase:                    PhaseIdle,
	}
}

func (m *MasterNode) RegisterWorker(workerID string, port string) {
	worker := &WorkerInfo{
		ID:     workerID,
		Port:   port,
		Active: true,
	}

	m.workers[workerID] = worker
}

func (m *MasterNode) LoadMapTasksFromSplits(splitDir string) error {
	files, err := os.ReadDir(splitDir)
	if err != nil {
		return fmt.Errorf("failed to read split directory: %w", err)
	}

	for i, f := range files {
		// Skip directories
		if f.IsDir() {
			continue
		}

		task := &TaskResponse{
			TaskID:    fmt.Sprintf("map-%d", i),
			TaskType:  "map",
			InputPath: filepath.Join(splitDir, f.Name()),
			Metadata: map[string]string{
				"numberOfReducers": fmt.Sprintf("%d", m.numberReducers),
				"pluginFile":       m.pluginfilepath,
			},
		}
		m.pendingTasks = append(m.pendingTasks, task)
	}

	if len(m.pendingTasks) == 0 {
		return fmt.Errorf("no valid files found in split directory")
	}

	m.phase = PhaseMap
	return nil
}

func (m *MasterNode) handleTaskStatusReport(report *TaskStatusReport) {
	// Remove from active task tracking
	delete(m.activeTasks, report.TaskID)

	if report.Success {
		fmt.Printf("[✓] Task %s completed by %s\n", report.TaskID, report.WorkerID)

		// Store intermediate files if any
		if len(report.IntermediateFiles) > 0 {
			for reducerID, filePath := range report.IntermediateFiles {
				m.reducerIntermediateFiles[reducerID] = append(m.reducerIntermediateFiles[reducerID], filePath)
			}
		}

		// Check if all map tasks are completed
		if len(m.pendingTasks) == 0 && len(m.activeTasks) == 0 {
			m.phase = PhaseIdle
		}

	} else {
		fmt.Printf("[✗] Task %s failed by %s. Error: %s\n", report.TaskID, report.WorkerID, report.Error)

		// Simple re-queue for now (you can enhance this later)
		failedTask := &TaskResponse{
			TaskID:    report.TaskID,
			TaskType:  "map",
			InputPath: "", // TODO: store original info for proper retry
			Metadata:  map[string]string{},
		}
		m.pendingTasks = append(m.pendingTasks, failedTask)
	}
}

func (m *MasterNode) StartScheduler() {
	go func() {
		for {
			select {
			case taskReq := <-m.requestChannel:
				if m.phase == PhaseMap && len(m.pendingTasks) > 0 {
					task := m.pendingTasks[0]
					m.pendingTasks = m.pendingTasks[1:]

					m.activeTasks[task.TaskID] = taskReq.WorkerID
					m.workerIdTaskMap[taskReq.WorkerID] = append(m.workerIdTaskMap[taskReq.WorkerID], task.TaskID)
					taskReq.ReplyCh <- task
				} else if m.phase == PhaseReduce {
					// Similar logic for reduce tasks
				} else {
					// Idle / No tasks available
					close(taskReq.ReplyCh)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case taskStatus := <-m.taskSubmissionChannel:
				m.handleTaskStatusReport(taskStatus)
			}
		}
	}()
}
