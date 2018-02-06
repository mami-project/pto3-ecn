// ecn_stabilizer is a local PTO analyzer that takes observations about paths
// from multiple raw observation sets, arranges them by target, and generates
// "stable" observations over time.

package main

import (
	"io"
	"log"
	"os"

	pto3 "github.com/mami-project/pto3-go"
)

func stabilizeECN(in io.Reader, out io.Writer) error {

	err := pto3.AnalyzeObservationStream(in, func(obs *pto3.Observation) error {
		// FIXME read observations and increment counters per condition
		return nil
	})

	if err != nil {
		return err
	}

	// FIXME iterate over VP/destination pairs and generate stable observations
	return nil

}

func main() {
	// just wrap stdin and stdout and go
	if err := stabilizeECN(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
