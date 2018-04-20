package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"github.com/calmh/ipfix"
)

type MetadataOut struct {
	TimeStart string `json:"_time_start"`
	TimeEnd   string `json:"_time_end"`
}

func main() {
	// force UTC (only works on Unix)
	os.Setenv("TZ", "")

	var timeStart, timeEnd, zeroTime time.Time

	// read IPFIX from stdin
	s := ipfix.NewSession()
	i := ipfix.NewInterpreter(s)

	for {
		msg, err := s.ParseReader(os.Stdin)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}

		// interpret records in each message into a map,
		// then pass them to the condition extractor
		for _, rec := range msg.DataRecords {
			startSeen := false
			endSeen := false
			fields := i.Interpret(rec)
			for _, field := range fields {
				if field.Name == "flowStartMilliseconds" {
					if (timeStart == zeroTime) ||
						(timeStart.Sub(field.Value.(time.Time)) > time.Duration(0)) {
						timeStart = field.Value.(time.Time)
					}
					if endSeen {
						break
					}
					startSeen = true
				} else if field.Name == "flowEndMilliseconds" {
					if (field.Value.(time.Time).Sub(timeEnd)) > time.Duration(0) {
						timeEnd = field.Value.(time.Time)
					}
					if startSeen {
						break
					}
					endSeen = true
				}
			}
		}
	}

	out := MetadataOut{
		TimeStart: timeStart.Format(time.RFC3339),
		TimeEnd:   timeEnd.Format(time.RFC3339),
	}

	b, err := json.Marshal(out)
	if err != nil {
		log.Fatal(err)
	}

	os.Stdout.Write(b)
}
