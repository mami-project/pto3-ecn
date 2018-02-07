// ecn_normalizer is a local normalizer (for use with ptonorm) that converts
// ndjson files from PathSpider (currently, version 1, though version 2
// support is planned) to PTO observations.

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

type psV1Observation struct {
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	Sip        string   `json:"sip"`
	Dip        string   `json:"dip"`
	Conditions []string `json:"conditions"`
}

func extractECNV1Observations(ndjsonLine string, sourceOverride string, sourcePrepend string) ([]pto3.Observation, error) {
	var psobs psV1Observation

	if err := json.Unmarshal([]byte(ndjsonLine), &psobs); err != nil {
		return nil, err
	}

	// try to parse timestamps
	formats := []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05"}

	var start, end time.Time
	var err error
	for _, timefmt := range formats {
		start, err = time.Parse(timefmt, psobs.Time.From)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot parse start time: %s", err.Error())
	}

	for _, timefmt := range formats {
		end, err = time.Parse(timefmt, psobs.Time.To)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot parse end time: %s", err.Error())
	}

	// make a path
	path := new(pto3.Path)

	var source string
	if sourceOverride != "" {
		source = sourceOverride
	} else {
		source = psobs.Sip
	}

	var pathElements []string
	if sourcePrepend != "" {
		if source != "" {
			pathElements = []string{sourcePrepend, source, "*", psobs.Dip}
		} else {
			pathElements = []string{sourcePrepend, "*", psobs.Dip}
		}
	} else {
		if source != "" {
			pathElements = []string{source, "*", psobs.Dip}
		} else {
			pathElements = []string{"*", psobs.Dip}
		}
	}

	path.String = strings.Join(pathElements, " ")

	// now create an observation for each condition
	obsen := make([]pto3.Observation, len(psobs.Conditions))
	for i, c := range psobs.Conditions {
		obsen[i].TimeStart = &start
		obsen[i].TimeEnd = &end
		obsen[i].Path = path
		obsen[i].Condition = new(pto3.Condition)
		obsen[i].Condition.Name = c
	}

	return obsen, nil
}

func normalizeECN(in io.Reader, metain io.Reader, out io.Writer) error {
	// unmarshal metadata into an RDS metadata object
	md, err := pto3.RawMetadataFromReader(metain, nil)
	if err != nil {
		return fmt.Errorf("could not read metadata: %s", err.Error())
	}

	// check filetype and select scanner
	var scanner *bufio.Scanner
	var extractFunc func(string, string, string) ([]pto3.Observation, error)

	switch md.Filetype(true) {
	case "pathspider-v1-ecn-ndjson":
		scanner = bufio.NewScanner(in)
		extractFunc = extractECNV1Observations
	case "pathspider-v1-ecn-ndjson-bz2":
		scanner = bufio.NewScanner(bzip2.NewReader(in))
		extractFunc = extractECNV1Observations
	default:
		return fmt.Errorf("unsupported filetype %s", md.Filetype(true))
	}

	// check for source override and prepend in metdata
	sourceOverride := md.Get("source_override", true)
	sourcePrepend := md.Get("source_prepend", true)

	// track conditions in the input
	hasCondition := make(map[string]bool)

	// now scan input for observations
	var lineno int
	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		switch line[0] {
		case '{':
			obsen, err := extractFunc(line, sourceOverride, sourcePrepend)
			if err != nil {
				return fmt.Errorf("error parsing PathSpider observation at line %d: %s", lineno, err.Error())
			}

			for _, o := range obsen {
				hasCondition[o.Condition.Name] = true
			}

			if err := pto3.WriteObservations(obsen, out); err != nil {
				return fmt.Errorf("error writing observation from line %d: %s", lineno, err.Error())
			}
		}
	}

	// now the metadata
	mdout := make(map[string]interface{})
	mdcond := make([]string, 0)

	// copy all aux metadata from the raw file
	for k := range md.Metadata {
		mdout[k] = md.Metadata[k]
	}

	// create condition list from observed conditions
	for k := range hasCondition {
		mdcond = append(mdcond, k)
	}
	mdout["_conditions"] = mdcond

	// add start and end time and owner, since we have it
	mdout["_owner"] = md.Owner
	mdout["_time_start"] = md.TimeStart(true).Format(time.RFC3339)
	mdout["_time_end"] = md.TimeEnd(true).Format(time.RFC3339)

	// hardcode analyzer path (FIXME, tag?)
	mdout["_analyzer"] = "https://github.com/mami-project/pto3-ecn/tree/master/ecn_normalizer/ecn_normalizer.json"

	// serialize and write to stdout
	b, err := json.Marshal(mdout)
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %s", err.Error())
	}

	if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
		return fmt.Errorf("error writing metadata: %s", err.Error())
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
