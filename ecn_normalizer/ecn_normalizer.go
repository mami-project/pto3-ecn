package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	pto3 "github.com/mami-project/pto3-go"
)

type PathspiderV1Observation struct {
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	Sip        string   `json:"sip"`
	Dip        string   `json:"dip"`
	Conditions []string `json:"conditions"`
}

func extractECNObservations(ndjsonLine string) ([]pto3.Observation, error) {
	var psobs PathspiderV1Observation

	if err := json.Unmarshal([]byte(ndjsonLine), &psobs); err != nil {
		return nil, err
	}

	// try to parse timestamps
	start, err := time.Parse("2006-01-02 15:04:05.000000", psobs.Time.From)
	if err != nil {
		return nil, fmt.Errorf("cannot parse start time: %s", err.Error())
	}
	end, err := time.Parse("2006-01-02 15:04:05.000000", psobs.Time.To)
	if err != nil {
		return nil, fmt.Errorf("cannot parse end time: %s", err.Error())
	}

	// make a path
	path := new(pto3.Path)
	path.String = fmt.Sprintf("%s * %s", psobs.Sip, psobs.Dip)

	// now create an observation for each condition
	obsen := make([]pto3.Observation, len(psobs.Conditions))
	for i, c := range psobs.Conditions {
		obsen[i].Start = start
		obsen[i].End = end
		obsen[i].Path = path
		obsen[i].Condition = new(pto3.Condition)
		obsen[i].Condition.Name = c
	}

	return obsen, nil
}
func normalizeECN(in io.Reader, metain io.Reader, out io.Writer) error {
	// unmarshal metadata into an RDS metadata object
	md, err := pto3.RDSMetadataFromReader(metain, nil)
	if err != nil {
		return fmt.Errorf("could not read metadata: %s", err.Error())
	}

	// check filetype and select scanner
	var scanner *bufio.Scanner
	switch md.Filetype() {
	case "ps-ecn-ndjson": // TODO is there a difference between pathsoider and pathspider2 here?
		scanner = bufio.NewScanner(in)
	case "ps-ecn-ndjson-bz2":
		scanner = bufio.NewScanner(bzip2.NewReader(in))
	default:
		return fmt.Errorf("unsupported filetype %s", md.Filetype())
	}

	// now scan input for observations
	var lineno int
	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		switch line[0] {
		case '{':
			// metadata. ignore.
			continue
		case '[':
			obsen, err := extractECNObservations(line)
			if err != nil {
				return fmt.Errorf("parse error for observation at line %d: %s", lineno, err.Error())
			}

			for _, obs := range obsen {
				b, err := json.Marshal(obs)
				if err != nil {
					return fmt.Errorf("cannon marshal observation at line %d: %s", lineno, err.Error())
				}

				if _, err = fmt.Fprintf(out, "%s\n", b); err != nil {
					return fmt.Errorf("error writing observation at line %d: %s", lineno, err.Error())
				}
			}
		}
	}

	return nil
}

func main() {
	// wrap a file around the metadata stream
	mdfile := os.NewFile(3, ".piped_metadata.json")

	// and go
	if err := normalizeECN(os.Stdin, mdfile, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
