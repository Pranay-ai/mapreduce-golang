package main

import (
	"go-mr/storage"
)

func main() {

	// Create a new Splitter instance with a chunk size of 128 KB
	splitter, err := storage.NewSplitter(1024*128, "meta/metadata.json")
	if err != nil {
		panic(err)
	}

	// Split the file "input.txt" and get the metadata
	meta, err := splitter.Split("input.txt")
	if err != nil {
		panic(err)
	}

	// Print the metadata for the split file
	println("File ID:", meta.FileID)
	println("Split Directory:", meta.SplitDir)
	// for _, chunk := range meta.Chunks {
	// 	println("Chunk:", chunk)
	// }

}
