package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pto3 "github.com/mami-project/pto3-go"
)

const metadataURL = "https://raw.githubusercontent.com/mami-project/pto3-ecn/master/normalize_pathspider/normalize_pathspider.json"

var conditionFixTable = map[string]string{
	"ecn.negotiated":     "ecn.negotiation.succeeded",
	"ecn.not_negotiated": "ecn.negotiation.failed",
	"ecn.ect_zero.seen":  "ecn.ipmark.ect0.seen",
	"ecn.ect_one.seen":   "ecn.ipmark.ect1.seen",
	"ecn.ce.seen":        "ecn.impark.ce.seen",
}

var timestampFormats = []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05"}

func fixCondition(cond string) (string, string) {

	// Split values at :
	cslice := strings.Split(cond, ":")
	cond = cslice[0]

	// Extract value
	var value string
	if len(cslice) > 1 {
		value = strings.Join(cslice[1:len(cslice)], ":")
	}

	// Rewrite old conditions to the new schema
	if newCond, ok := conditionFixTable[cond]; ok {
		cond = newCond
	}

	return cond, value
}

type timestampPair struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func parseTimestamps(in timestampPair) (time.Time, time.Time, error) {
	var start, end time.Time
	var err error
	for _, timefmt := range timestampFormats {
		start, err = time.Parse(timefmt, in.From)
		if err == nil {
			break
		}
	}
	if err != nil {
		return start, end, fmt.Errorf("cannot parse start time: %s", err.Error())
	}

	for _, timefmt := range timestampFormats {
		end, err = time.Parse(timefmt, in.To)
		if err == nil {
			break
		}
	}
	if err != nil {
		return start, end, fmt.Errorf("cannot parse end time: %s", err.Error())
	}

	return start, end, nil
}

type psV1Observation struct {
	Time       timestampPair `json:"time"`
	Sip        string        `json:"sip"`
	Dip        string        `json:"dip"`
	Conditions []string      `json:"conditions"`
}

func normalizeV1(rec string, mdin *pto3.RawMetadata, mdout map[string]interface{}) ([]pto3.Observation, error) {
	var psobs psV1Observation

	// parse ndjson line
	if err := json.Unmarshal([]byte(rec), &psobs); err != nil {
		return nil, err
	}

	// parse timestamps
	var start, end time.Time
	var err error
	if start, end, err = parseTimestamps(psobs.Time); err != nil {
		return nil, err
	}

	// make a path
	sourceOverride := mdin.Get("source_override", true)
	sourcePrepend := mdin.Get("source_prepend", true)
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
		cond, value := fixCondition(c)
		obsen[i].Condition.Name = cond
		obsen[i].Value = value
	}

	return obsen, nil
}

type psV2Observation struct {
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"time"`
	Path       []string `json:"path"`
	Conditions []string `json:"conditions"`
	CanidInfo  struct {
		ASN uint32 `json:"ASN"`
	} `json:"canid_info"`
}

func normalizeV2(rec string, mdin *pto3.RawMetadata, mdout map[string]interface{}) ([]pto3.Observation, error) {
	var psobs psV2Observation

	// parse ndjson line
	if err := json.Unmarshal([]byte(rec), &psobs); err != nil {
		return nil, err
	}

	// parse timestamps
	var start, end time.Time
	var err error
	if start, end, err = parseTimestamps(psobs.Time); err != nil {
		return nil, err
	}

	// edit path: source override and prepend,
	// add * if missing, extract ASN from Canid information if present
	sourceOverride := mdin.Get("source_override", true)
	sourcePrepend := mdin.Get("source_prepend", true)

	if psobs.Path != nil && len(psobs.Path) >= 2 {
		if sourceOverride != "" {
			psobs.Path[0] = sourceOverride
		}

		if sourcePrepend != "" {
			psobs.Path = append([]string{sourcePrepend}, psobs.Path...)
		}

		star := ""
		if len(psobs.Path) > 2 && psobs.Path[len(psobs.Path)-2] != "*" {
			star = "*"
		}

		canidAS := ""
		if psobs.CanidInfo.ASN != 0 {
			canidAS = fmt.Sprintf("AS%d", psobs.CanidInfo.ASN)
		}

		switch {
		case star != "" && canidAS != "":
			dip := psobs.Path[len(psobs.Path)-1]
			psobs.Path[len(psobs.Path)-1] = star
			psobs.Path = append(psobs.Path, canidAS)
			psobs.Path = append(psobs.Path, dip)
		case star == "" && canidAS != "":
			dip := psobs.Path[len(psobs.Path)-1]
			psobs.Path[len(psobs.Path)-1] = canidAS
			psobs.Path = append(psobs.Path, dip)
		case star != "" && canidAS == "":
			dip := psobs.Path[len(psobs.Path)-1]
			psobs.Path[len(psobs.Path)-1] = star
			psobs.Path = append(psobs.Path, dip)
		}
	} else {
		return nil, fmt.Errorf("bad or missing path")
	}

	path := new(pto3.Path)
	path.String = strings.Join(psobs.Path, " ")

	// now create an observation for each condition
	obsen := make([]pto3.Observation, len(psobs.Conditions))
	for i, c := range psobs.Conditions {
		obsen[i].TimeStart = &start
		obsen[i].TimeEnd = &end
		obsen[i].Path = path
		obsen[i].Condition = new(pto3.Condition)
		cond, value := fixCondition(c)
		obsen[i].Condition.Name = cond
		obsen[i].Value = value
	}

	return obsen, nil
}

func main() {
	// wrap a file around the metadata stream
	mdfile := os.NewFile(3, ".piped_metadata.json")

	// create a scanning normalizer
	sn := pto3.NewScanningNormalizer(metadataURL)
	sn.RegisterFiletype("pathspider-v1-ecn-ndjson", bufio.ScanLines, normalizeV1, nil)
	sn.RegisterFiletype("pathspider-v2-ndjson", bufio.ScanLines, normalizeV2, nil)

	// and run it
	log.Fatal(sn.Normalize(os.Stdin, mdfile, os.Stdout))
}
