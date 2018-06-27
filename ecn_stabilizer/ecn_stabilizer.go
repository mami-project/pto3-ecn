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

	ecn "github.com/mami-project/pto3-ecn"
	pto3 "github.com/mami-project/pto3-go"
)

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
	stableTable := make(map[string]*ecn.CondCount)

	obsCount := 0

	// analyze the observation stream into the table
	setTable, err := pto3.AnalyzeObservationStream(in, func(obs *pto3.Observation) error {

		var pathkey string
		vp := obs.Set.Metadata["vantage"]
		if vp == "" {
			pathkey = obs.Path.String
		} else {
			pathkey = vp + " * " + obs.Path.Target
		}

		counters := stableTable[pathkey]

		if counters == nil {
			counters = new(ecn.CondCount)
			stableTable[pathkey] = counters
		}

		// add this observation to the counters
		counters.Observe(obs)

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

	// track conditions
	conditionSeen := make(pto3.ConditionSet)

	// now iterate over VP/destination pairs and generate stable observations
	for pathkey := range stableTable {
		var obsval int

		entry := stableTable[pathkey]

		cobs := pto3.Observation{
			TimeStart: entry.TimeStart,
			TimeEnd:   entry.TimeEnd,
			Path:      &pto3.Path{String: pathkey},
		}

		switch {
		case entry.ConnWorks > 0 && entry.ConnBroken == 0:
			cobs.Condition = connStableWorks
			obsval = entry.ConnWorks
		case entry.ConnBroken > 0 && entry.ConnWorks == 0 && entry.ConnTransient == 0:
			cobs.Condition = connStableBroken
			obsval = entry.ConnBroken
		case entry.ConnWorks+entry.ConnBroken+entry.ConnTransient == 0:
			cobs.Condition = connStableOffline
			obsval = entry.ConnOffline
		case entry.ConnWorks+entry.ConnBroken == 0:
			cobs.Condition = connStableTransient
			obsval = entry.ConnTransient
		default:
			cobs.Condition = connUnstable
			obsval = 0
		}

		cobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen.AddCondition(cobs.Condition.Name)

		nobs := pto3.Observation{
			TimeStart: entry.TimeStart,
			TimeEnd:   entry.TimeEnd,
			Path:      &pto3.Path{String: pathkey},
		}

		switch {
		case entry.NegoWorks > 0 && entry.NegoFailed == 0 && entry.NegoReflected == 0:
			nobs.Condition = negoStableWorks
			obsval = entry.NegoWorks
		case entry.NegoFailed > 0 && entry.NegoWorks == 0 && entry.NegoReflected == 0:
			nobs.Condition = negoStableFailed
			obsval = entry.NegoFailed
		case entry.NegoReflected > 0 && entry.NegoWorks == 0 && entry.NegoFailed == 0:
			nobs.Condition = negoStableReflected
			obsval = entry.NegoReflected
		default:
			nobs.Condition = negoUnstable
			obsval = 0
		}

		nobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen.AddCondition(nobs.Condition.Name)

		obsen := []pto3.Observation{cobs, nobs}
		if err := pto3.WriteObservations(obsen, out); err != nil {
			return err
		}
	}

	// and now the metadata
	mdout := setTable.MergeMetadata()

	// list conditions
	mdout["_conditions"] = conditionSeen.Conditions()

	// hardcode analyzer path
	mdout["_analyzer"] = "https://raw.githubusercontent.com/mami-project/pto3-ecn/master/ecn_stabilizer/ecn_stabilizer.json"

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
	// just wrap stdin and stdout and go
	if err := stabilizeECN(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
