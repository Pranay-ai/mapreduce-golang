package mapreducese

import (
	"flag"
	"fmt"
	mapreducese "go-mr/mapreduce-se"
	"log"
	"os"
)

func main() {
	inputFile := flag.String("input", "", "path to input file for processing")
	pluginFile := flag.String("plugin", "", "path to .so plugin containing Map and Reduce")
	outputFile := flag.String("output", "", "path to output file (optional, prints to console if not provided)")

	flag.Parse()

	if *inputFile == "" || *pluginFile == "" {
		flag.Usage()
		return
	}

	fmt.Println("Input file is:", *inputFile)
	fmt.Println("Plugin file is:", *pluginFile)

	mr := mapreducese.MapReduceSequential{}

	if err := mr.LoadMapper(*pluginFile); err != nil {
		log.Fatalf("Failed to load mapper: %v", err)
	}

	if err := mr.LoadReducer(*pluginFile); err != nil {
		log.Fatalf("Failed to load reducer: %v", err)
	}

	// Open the input file
	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close() // Important: always close the file when done

	// Run the MapReduce process
	result, err := mr.Run(file)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	}

	// Output results - either to file or console
	if *outputFile != "" {
		// Write to file
		outFile, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outFile.Close()

		fmt.Fprintln(outFile, "MapReduce Results:")
		for key, value := range result {
			fmt.Fprintf(outFile, "%s: %s\n", key, value)
		}
		fmt.Printf("Results written to %s\n", *outputFile)
	} else {
		// Print to console (existing behavior)
		fmt.Println("MapReduce Results:")
		for key, value := range result {
			fmt.Printf("%s: %s\n", key, value)
		}
	}
}
