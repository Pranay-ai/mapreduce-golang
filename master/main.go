package master

import (
	"flag"
	"fmt"
	"go-mr/masterapi"
	"go-mr/storage"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

func main() {
	// Command line flags
	var (
		inputFile    = flag.String("input", "", "Input file path to process")
		pluginFile   = flag.String("plugin", "", "Plugin file path for map/reduce functions")
		outputDir    = flag.String("output", "/Volumes/mapreduce_storage/output", "Output directory")
		port         = flag.String("port", "8080", "Master server port")
		nReducers    = flag.Int("reducers", 3, "Number of reduce tasks")
		chunkSize    = flag.Int("chunk-size", 1024*1024, "Chunk size in bytes for splitting")
		metadataPath = flag.String("metadata", "/Volumes/mapreduce_storage/metadata.json", "Metadata file path")
	)
	flag.Parse()

	// Validate required flags
	if *inputFile == "" {
		log.Fatal("Input file is required. Use -input flag")
	}
	if *pluginFile == "" {
		log.Fatal("Plugin file is required. Use -plugin flag")
	}

	// Check if input file exists
	if _, err := os.Stat(*inputFile); os.IsNotExist(err) {
		log.Fatalf("Input file does not exist: %s", *inputFile)
	}
	if _, err := os.Stat(*pluginFile); os.IsNotExist(err) {
		log.Fatalf("Plugin file does not exist: %s", *pluginFile)
	}

	fmt.Printf("Starting MapReduce Master Node\n")
	fmt.Printf("Input file: %s\n", *inputFile)
	fmt.Printf("Plugin file: %s\n", *pluginFile)
	fmt.Printf("Output directory: %s\n", *outputDir)
	fmt.Printf("Number of reducers: %d\n", *nReducers)
	fmt.Printf("Server port: %s\n", *port)

	// Initialize file splitter
	splitter, err := storage.NewSplitter(*chunkSize, *metadataPath)
	if err != nil {
		log.Fatalf("Failed to create splitter: %v", err)
	}

	// Split the input file
	fmt.Printf("Splitting input file into chunks...\n")
	metadata, err := splitter.Split(*inputFile)
	if err != nil {
		log.Fatalf("Failed to split input file: %v", err)
	}
	fmt.Printf("File split into %d chunks in directory: %s\n", len(metadata.Chunks), metadata.SplitDir)

	// Create master node
	masterNode := NewMasterNode(*inputFile, *pluginFile, *outputDir, *nReducers)

	// Load map tasks from the split files
	if err := masterNode.LoadMapTasksFromSplits(metadata.SplitDir); err != nil {
		log.Fatalf("Failed to load map tasks: %v", err)
	}

	// Start the master scheduler
	masterNode.StartScheduler()
	fmt.Printf("Master scheduler started\n")

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	masterApiServer := NewMasterApiServer(masterNode)
	masterapi.RegisterMasterApiServer(grpcServer, masterApiServer)

	// Start gRPC server in a goroutine
	go func() {
		fmt.Printf("Master gRPC server listening on port %s\n", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Print status and wait
	fmt.Printf("Master node is running. Press Ctrl+C to stop.\n")
	fmt.Printf("Workers can connect to: localhost:%s\n", *port)

	// Block until we receive a signal
	<-sigChan
	fmt.Printf("\nShutting down master node...\n")

	// Graceful shutdown
	grpcServer.GracefulStop()
	fmt.Printf("Master node stopped.\n")
}

// Optional: Add a status endpoint or monitoring
