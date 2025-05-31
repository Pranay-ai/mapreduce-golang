package worker

import (
	"context"
	"go-mr/workerapi"
)

type WorkerApiServer struct {
	workerapi.UnimplementedWorkerApiServer
}

func (ws *WorkerApiServer) HealthCheck(ctx context.Context, req *workerapi.HealthCheckRequest) (*workerapi.HealthCheckResponse, error) {
	// Here we would typically check the worker's health status.
	// For simplicity, we return a healthy status.
	return &workerapi.HealthCheckResponse{
		Healthy: true,
		Message: "Worker is healthy",
	}, nil
}
