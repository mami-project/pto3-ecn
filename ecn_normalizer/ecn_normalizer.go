package main

import (
	"log"
	"os"
	"time"
)

const MetadataFd = 3

type RawECNMetadata struct {
	Filetype  string     `json:"_file_type"`
	TimeStart *time.Time `json:"_time_start"`
	TimeEnd   *time.Time `json:"_time_end"`
	Metadata  map[string]string
}

type ECNObservationSetMetadata struct {
	Conditions []string `json:"_conditions"`
	Sources    []string `json:"_sources"`
}

func main() {

	// grab standard metadata
	stdmd := os.NewFile(MetadataFd, "stdmd.json")
	if stdmd == nil {
		log.Fatalf("no metadata available on file descriptor %d", MetadataFd)
	}

	// WORK POINTER

}
