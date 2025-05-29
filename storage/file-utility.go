package storage

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// InputFileMetadata represents metadata for one input file.
type InputFileMetadata struct {
	FileID   string   `json:"file_id"`
	SplitDir string   `json:"split_dir"`
	Chunks   []string `json:"chunks"`
}

// Splitter encapsulates the logic for file splitting and metadata handling.
type Splitter struct {
	ChunkSize    int
	MetadataPath string
	Metadata     map[string]InputFileMetadata
}

// NewSplitter creates a new instance of Splitter and loads metadata if present.
func NewSplitter(chunkSize int, metadataPath string) (*Splitter, error) {
	metadata := make(map[string]InputFileMetadata)
	data, err := os.ReadFile(metadataPath)
	if err == nil {
		if err := json.Unmarshal(data, &metadata); err != nil {
			return nil, fmt.Errorf("failed to parse metadata: %v", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	return &Splitter{
		ChunkSize:    chunkSize,
		MetadataPath: metadataPath,
		Metadata:     metadata,
	}, nil
}

// hashFileName generates a short hash based on the base name of a file.
func (s *Splitter) hashFileName(filePath string) string {
	h := sha1.New()
	h.Write([]byte(filepath.Base(filePath)))
	return hex.EncodeToString(h.Sum(nil))[:6]
}

// Split splits the file and updates metadata.
func (s *Splitter) Split(filePath string) (*InputFileMetadata, error) {
	if meta, exists := s.Metadata[filePath]; exists {
		fmt.Printf("Skipping split for %s (already registered)\n", filePath)
		return &meta, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentChunk []byte
	currentSize := 0
	chunkIndex := 0

	fileID := fmt.Sprintf("input-%s", s.hashFileName(filePath))
	outputDir := filepath.Join("/Volumes/mapreduce_storage/splits", fileID)

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create split directory: %v", err)
	}

	meta := &InputFileMetadata{
		FileID:   fileID,
		SplitDir: outputDir,
		Chunks:   []string{},
	}

	for scanner.Scan() {
		line := scanner.Text()
		lineBytes := append([]byte(line), '\n')

		if currentSize+len(lineBytes) > s.ChunkSize && currentSize > 0 {
			chunkPath := filepath.Join(outputDir, fmt.Sprintf("chunk-%04d.txt", chunkIndex))
			if err := os.WriteFile(chunkPath, currentChunk, 0644); err != nil {
				return nil, fmt.Errorf("failed to write chunk: %v", err)
			}
			meta.Chunks = append(meta.Chunks, chunkPath)
			chunkIndex++
			currentChunk = []byte{}
			currentSize = 0
		}

		currentChunk = append(currentChunk, lineBytes...)
		currentSize += len(lineBytes)
	}

	if len(currentChunk) > 0 {
		chunkPath := filepath.Join(outputDir, fmt.Sprintf("chunk-%04d.txt", chunkIndex))
		if err := os.WriteFile(chunkPath, currentChunk, 0644); err != nil {
			return nil, fmt.Errorf("failed to write last chunk: %v", err)
		}
		meta.Chunks = append(meta.Chunks, chunkPath)
	}

	s.Metadata[filePath] = *meta
	if err := s.saveMetadata(); err != nil {
		return nil, err
	}

	return meta, nil
}

// saveMetadata writes the updated metadata to disk.
func (s *Splitter) saveMetadata() error {
	data, err := json.MarshalIndent(s.Metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(s.MetadataPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create metadata directory: %v", err)
	}

	if err := os.WriteFile(s.MetadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %v", err)
	}

	return nil
}
