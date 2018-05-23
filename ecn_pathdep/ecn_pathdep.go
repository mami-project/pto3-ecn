// ecn_pathdep is a local PTO analyzer that takes observations about paths
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

func pathdepECN(in io.Reader, out io.Writer) error {

	// create some conditions
	connMPWorks := &pto3.Condition{Name: "ecn.multipoint.connectivity.works"}
	connMPBroken := &pto3.Condition{Name: "ecn.multipoint.connectivity.broken"}
	connMPOffline := &pto3.Condition{Name: "ecn.multipoint.connectivity.offline"}
	connMPTransient := &pto3.Condition{Name: "ecn.multipoint.connectivity.transient"}
	connMPPathDep := &pto3.Condition{Name: "ecn.multipoint.connectivity.path_dependent"}
	connMPUnstable := &pto3.Condition{Name: "ecn.multipoint.connectivity.unstable"}

	negoMPWorks := &pto3.Condition{Name: "ecn.multipoint.negotiation.succeeded"}
	negoMPFailed := &pto3.Condition{Name: "ecn.multipoint.negotiation.failed"}
	negoMPReflected := &pto3.Condition{Name: "ecn.multipoint.negotiation.reflected"}
	negoMPPathDep := &pto3.Condition{Name: "ecn.multipoint.negotiation.path_dependent"}
	negoMPUnstable := &pto3.Condition{Name: "ecn.multipoint.negotiation.unstable"}

	// map targets to sources to condition counts
	mvTable := make(map[string]map[string]*ecn.CondCount)

	// create a set table (for metadata generation)
	setTable := make(ecn.SetTable)

	obsCount := 0

	// analyze the observation stream into the tables
	err := pto3.AnalyzeObservationStream(in, func(obs *pto3.Observation) error {

		setTable.AddSetFrom(obs)

		countmap := mvTable[obs.Path.Target]
		if countmap == nil {
			countmap = make(map[string]*ecn.CondCount)
			mvTable[obs.Path.Target] = countmap
		}

		counters := countmap[obs.Path.Source]
		if counters == nil {
			counters = new(ecn.CondCount)
			countmap[obs.Path.Source] = counters
		}

		// add this observation to the counters
		counters.Observe(obs)

		obsCount++
		if obsCount%100000 == 0 {
			log.Printf("ecn_pathdep debug observation %d tablesize %d", obsCount, len(mvTable))
		}

		return nil
	})

	// check for observation read error
	if err != nil {
		return err
	}

	// track conditions
	conditionSeen := make(ecn.ConditionSet)

	// iterate over targets, looking for different outcomes from different sources
	for target := range mvTable {
		var obsval int

		countmap := mvTable[target]
		a := new(ecn.CondCount)

		for source := range countmap {
			a.Add(countmap[source])
		}

		cobs := pto3.Observation{
			TimeStart: a.TimeStart,
			TimeEnd:   a.TimeEnd,
			Path:      &pto3.Path{String: "* " + target},
		}

		switch {
		case a.ConnBroken > 0 && a.ConnWorks > 0:
			cobs.Condition = connMPPathDep
			obsval = len(countmap)
		case a.ConnWorks > 0 && a.ConnBroken+a.ConnTransient == 0:
			cobs.Condition = connMPWorks
			obsval = a.ConnWorks
		case a.ConnBroken > 0 && a.ConnWorks+a.ConnTransient == 0:
			cobs.Condition = connMPBroken
			obsval = a.ConnWorks
		case a.ConnTransient > 0 && a.ConnBroken+a.ConnWorks == 0:
			cobs.Condition = connMPTransient
			obsval = a.ConnTransient
		case a.ConnOffline > 0 && a.ConnWorks+a.ConnBroken+a.ConnTransient == 0:
			cobs.Condition = connMPOffline
			obsval = a.ConnOffline
		default:
			cobs.Condition = connMPUnstable
			obsval = 0
		}

		cobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen.AddCondition(cobs.Condition.Name)

		nobs := pto3.Observation{
			TimeStart: a.TimeStart,
			TimeEnd:   a.TimeEnd,
			Path:      &pto3.Path{String: "* " + target},
		}

		switch {
		case a.NegoWorks > 0 && a.NegoFailed > 0:
			nobs.Condition = negoMPPathDep
			obsval = len(countmap)
		case a.NegoWorks > 0 && a.NegoFailed+a.NegoReflected == 0:
			nobs.Condition = negoMPWorks
			obsval = a.NegoWorks
		case a.NegoFailed > 0 && a.NegoWorks+a.NegoReflected == 0:
			nobs.Condition = negoMPFailed
			obsval = a.NegoFailed
		case a.NegoReflected > 0 && a.NegoWorks+a.NegoFailed == 0:
			nobs.Condition = negoMPReflected
			obsval = a.NegoReflected
		default:
			nobs.Condition = negoMPUnstable
			obsval = 0
		}

		nobs.Value = fmt.Sprintf("%d", obsval)
		conditionSeen.AddCondition(nobs.Condition.Name)

		// write observations
		obsen := []pto3.Observation{cobs, nobs}
		if err := pto3.WriteObservations(obsen, out); err != nil {
			return err
		}
	}

	// merge metadata from set
	mdout := setTable.MergeMetadata()

	// add conditions
	mdout["_conditions"] = conditionSeen.Conditions()

	// hardcode analyzer path
	mdout["_analyzer"] = "https://github.com/mami-project/pto3-ecn/tree/master/ecn_pathdep/ecn_pathdep.json"

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
	if err := pathdepECN(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
