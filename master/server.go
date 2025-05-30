package master

import (
	"context"
	"fmt"
	"go-mr/masterapi"
)

type MasterApiServer struct {
	masterapi.UnimplementedMasterApiServer
	master *MasterNode
}

func NewMasterApiServer(master *MasterNode) *MasterApiServer {
	return &MasterApiServer{
		master: master,
	}
}

func (ms *MasterApiServer) RegisterWorker(ctx context.Context, req *masterapi.RegisterWorkerRequest) (*masterapi.RegisterWorkerResponse, error) {
	workerId := req.GetWorkerid()
	workerPort := req.GetWorkerport()

	// Basic validation
	if workerId == "" {
		return nil, fmt.Errorf("worker ID cannot be empty")
	}
	if workerPort == "" {
		return nil, fmt.Errorf("worker port cannot be empty")
	}

	ms.master.RegisterWorker(workerId, workerPort)
	return &masterapi.RegisterWorkerResponse{}, nil
}

func (ms *MasterApiServer) RequestTask(ctx context.Context, req *masterapi.TaskRequest) (*masterapi.TaskResponse, error) {
	workerId := req.GetWorkerid()

	// Validate worker ID
	if workerId == "" {
		return nil, fmt.Errorf("worker ID cannot be empty")
	}

	replyChan := make(chan *TaskResponse)
	taskRequest := &TaskRequest{
		WorkerID: workerId,
		ReplyCh:  replyChan,
	}

	// Send the task request to the master's scheduling loop
	select {
	case ms.master.requestChannel <- taskRequest:
		// Request sent successfully
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for the task assignment with context cancellation support
	select {
	case taskResp, ok := <-replyChan:
		if !ok {
			// No tasks available - channel was closed
			return &masterapi.TaskResponse{
				Taskid:    "",
				Tasktype:  "none",
				Inputpath: "",
				Metadata:  make(map[string]string),
			}, nil
		}

		// Return the task to the worker
		return &masterapi.TaskResponse{
			Taskid:    taskResp.TaskID,
			Tasktype:  taskResp.TaskType,
			Inputpath: taskResp.InputPath,
			Metadata:  taskResp.Metadata,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ms *MasterApiServer) ReportTaskStatus(ctx context.Context, req *masterapi.TaskStatusReport) (*masterapi.TaskStatusAck, error) {
	taskID := req.GetTaskid()
	workerID := req.GetWorkerid()
	success := req.GetSuccess()
	errorMsg := req.GetError()
	intermediateFiles := req.GetIntermediatefiles()

	// Basic validation
	if taskID == "" {
		return &masterapi.TaskStatusAck{Success: false}, fmt.Errorf("task ID cannot be empty")
	}
	if workerID == "" {
		return &masterapi.TaskStatusAck{Success: false}, fmt.Errorf("worker ID cannot be empty")
	}

	// Construct internal struct
	report := &TaskStatusReport{
		WorkerID:          workerID,
		TaskID:            taskID,
		Success:           success,
		Error:             errorMsg,
		IntermediateFiles: intermediateFiles,
	}

	// Send to master's taskSubmissionChannel for processing with context support
	select {
	case ms.master.taskSubmissionChannel <- report:
		return &masterapi.TaskStatusAck{Success: true}, nil
	case <-ctx.Done():
		return &masterapi.TaskStatusAck{Success: false}, ctx.Err()
	}
}
