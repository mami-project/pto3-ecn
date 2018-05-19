// ecn_stabilizer is a local PTO analyzer that takes observations about paths
// from multiple raw observation sets, arranges them by target, and generates
// "stable" observations over time.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pto3 "github.com/mami-project/pto3-go"
)

type condCount struct {
	timeStart     *time.Time
	timeEnd       *time.Time
	total         int
	connWorks     int
	connBroken    int
	connTransient int
	connOffline   int
	negoWorks     int
	negoFailed    int
	negoReflected int
}

func stabilizeECN(in io.Reader, out io.Writer) error {

	// create some conditions
	connStableWorks := &pto3.Condition{Name: "ecn.stable.connectivity.works"}
	connStableBroken := &pto3.Condition{Name: "ecn.stable.connectivity.broken"}
	connStableOffline := &pto3.Condition{Name: "ecn.stable.connectivity.offline"}
	connStableTransient := &pto3.Condition{Name: "ecn.stable.connectivity.transient"}
	connUnstable := &pto3.Condition{Name: "ecn.stable.connectivity.unstable"}

	negoStableWorks := &pto3.Condition{Name: "ecn.stable.negotiation.succeeded"}
	negoStableFailed := &pto3.Condition{Name: "ecn.stable.negotiation.failed"}
	negoStableReflected := &pto3.Condition{Name: "ecn.stable.negotiation.reflected"}
	negoUnstable := &pto3.Condition{Name: "ecn.stable.negotiation.unstable"}

	// create a table mapping targets to condition counters
	stableTable := make(map[string]*condCount)

	// create a set table (for metadata generation)
	setTable := make(map[int]*pto3.ObservationSet)

	obsCount := 0

	// analyze the observation stream into the table
	err := pto3.AnalyzeObservationStream(in, func(obs *pto3.Observation) error {

		if _, ok := setTable[obs.SetID]; !ok {
			setTable[obs.SetID] = obs.Set
		}

		var pathkey string
		vp := obs.Set.Metadata["vantage"]
		if vp == "" {
			pathkey = obs.Path.String
		} else {
			pathkey = vp + " * " + obs.Path.Target
		}

		counters := stableTable[pathkey]

		if counters == nil {
			counters = new(condCount)
			stableTable[pathkey] = counters
		}

		if counters.total == 0 {
			counters.timeStart = obs.TimeStart
			counters.timeEnd = obs.TimeEnd
		} else {
			if counters.timeStart.Sub(*obs.TimeStart) > 0 {
				counters.timeStart = obs.TimeStart
			}
			if obs.TimeEnd.Sub(*counters.timeEnd) > 0 {
				counters.timeEnd = obs.TimeEnd
			}
		}

		counters.total++

		switch obs.Condition.Name {
		case "ecn.connectivity.works":
			counters.connWorks++
		case "ecn.connectivity.broken":
			counters.connBroken++
		case "ecn.connectivity.transient":
			counters.connTransient++
		case "ecn.connectivity.offline":
			counters.connOffline++
		case "ecn.negotiation.succeeded":
			counters.negoWorks++
		case "ecn.negotiated":
			counters.negoWorks++
		case "ecn.negotiation.failed":
			counters.negoFailed++
		case "ecn.not_negotiated":
			counters.negoFailed++
		case "ecn.negotiation.reflected":
			counters.negoReflected++
		default:
			counters.total--
		}

		obsCount++
		if obsCount%100000 == 0 {
			log.Printf("ecn_stabilizer debug observation %d pathkey %s tablesize %d", obsCount, pathkey, len(stableTable))
		}

		return nil
	})

	// check for observation read error
	if err != nil {
		return err
	}

	conditionSeen := make(map[string]struct{})

	// now iterate over VP/destination pairs and generate stable observations
	for pathkey := range stableTable {
		var obsval int

		entry := stableTable[pathkey]

		cobs := pto3.Observation{
			TimeStart: entry.timeStart,
			TimeEnd:   entry.timeEnd,
			Path:      &pto3.Path{String: pathkey},
		}

		if entry.connWorks > 0 && entry.connBroken == 0 {
			cobs.Condition = connStableWorks
			obsval = entry.connWorks
		} else if entry.connBroken > 0 && entry.connWorks == 0 && entry.connTransient == 0 {
			cobs.Condition = connStableBroken
			obsval = entry.connBroken
		} else if entry.connWorks+entry.connBroken+entry.connTransient == 0 {
			cobs.Condition = connStableOffline
			obsval = entry.connOffline
		} else if entry.connWorks+entry.connBroken == 0 {
			cobs.Condition = connStableTransient
			obsval = entry.connTransient
		} else {
			cobs.Condition = connUnstable
			obsval = 0
		}

		cobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen[cobs.Condition.Name] = struct{}{}

		nobs := pto3.Observation{
			TimeStart: entry.timeStart,
			TimeEnd:   entry.timeEnd,
			Path:      &pto3.Path{String: pathkey},
		}

		if entry.negoWorks > 0 && entry.negoFailed == 0 && entry.negoReflected == 0 {
			nobs.Condition = negoStableWorks
			obsval = entry.negoWorks
		} else if entry.negoFailed > 0 && entry.negoWorks == 0 && entry.negoReflected == 0 {
			nobs.Condition = negoStableFailed
			obsval = entry.negoFailed
		} else if entry.negoReflected > 0 && entry.negoWorks == 0 && entry.negoFailed == 0 {
			nobs.Condition = negoStableReflected
			obsval = entry.negoReflected
		} else {
			nobs.Condition = negoUnstable
			obsval = 0
		}

		nobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen[nobs.Condition.Name] = struct{}{}

		obsen := []pto3.Observation{cobs, nobs}
		if err := pto3.WriteObservations(obsen, out); err != nil {
			return err
		}
	}

	// and now the metadata
	mdout := make(map[string]interface{})

	// track sources and inherit arbitrary metadata for all keys without conflict
	sources := make([]string, 0)
	conflictingKeys := make(map[string]struct{})

	for setid := range setTable {

		source := setTable[setid].Link()
		if source != "" {
			sources = append(sources, source)
		}

		for k, newval := range setTable[setid].Metadata {
			if _, ok := conflictingKeys[k]; ok {
				continue
			} else {
				existval, ok := mdout[k]
				if !ok {
					mdout[k] = newval
				} else if fmt.Sprintf("%v", existval) != fmt.Sprintf("%v", newval) {
					delete(mdout, k)
					conflictingKeys[k] = struct{}{}
				}
			}
		}
	}

	// list conditions
	mdcond := make([]string, 0)
	for k := range conditionSeen {
		mdcond = append(mdcond, k)
	}
	mdout["_conditions"] = mdcond

	// track sources
	if len(sources) > 0 {
		mdout["_sources"] = sources
	}

	// hardcode analyzer path
	mdout["_analyzer"] = "https://github.com/mami-project/pto3-ecn/tree/master/ecn_stabilizer/ecn_stabilizer.json"

	// serialize and write to stdout
	b, err := json.Marshal(mdout)
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %s", err.Error())
	}

	if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
		return fmt.Errorf("error writing metadata: %s", err.Error())
	}

	// dump the counters table to a table file (debugging)

	// dumpfile, err := os.Create("ecn_stabilizer_table.csv")
	// if err != nil {
	// 	log.Fatalf("cannot open dumpfile: %v", err)
	// }
	// defer dumpfile.Close()

	// for pathkey := range stableTable {
	// 	entry := stableTable[pathkey]

	// 	fmt.Fprintf(dumpfile,
	// 		"%s,%d,%d,%d,%d,%d,%d,%d,%d\n",
	// 		pathkey, entry.total,
	// 		entry.connWorks, entry.connBroken,
	// 		entry.connTransient, entry.connOffline,
	// 		entry.negoWorks, entry.negoFailed, entry.negoReflected)
	// }

	return nil
}

func main() {
	// just wrap stdin and stdout and go
	if err := stabilizeECN(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
