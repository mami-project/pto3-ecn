package ecn

import (
	"strconv"
	"strings"
	"time"

	pto3 "github.com/mami-project/pto3-go"
)

type CondCount struct {
	TimeStart     *time.Time
	TimeEnd       *time.Time
	Total         int
	ConnWorks     int
	ConnBroken    int
	ConnTransient int
	ConnOffline   int
	ConnUnstable  int
	NegoWorks     int
	NegoFailed    int
	NegoReflected int
	NegoUnstable  int
	IpEct0        int
	IpEct1        int
	IpCe          int
	NoIpEct0      int
	NoIpEct1      int
	NoIpCe        int
}

func (cc *CondCount) Observe(obs *pto3.Observation) {
	if cc.Total == 0 {
		cc.TimeStart = obs.TimeStart
		cc.TimeEnd = obs.TimeEnd
	} else {
		if cc.TimeStart.Sub(*obs.TimeStart) > 0 {
			cc.TimeStart = obs.TimeStart
		}
		if obs.TimeEnd.Sub(*cc.TimeEnd) > 0 {
			cc.TimeEnd = obs.TimeEnd
		}
	}

	cc.Total++

	var increment int
	if strings.HasPrefix(obs.Condition.Name, "ecn.stable.") {
		increment, _ = strconv.Atoi(obs.Value)
	} else {
		increment = 1
	}

	switch obs.Condition.Name {
	case "ecn.connectivity.works", "ecn.stable.connectivity.works":
		cc.ConnWorks += increment
	case "ecn.connectivity.broken", "ecn.stable.connectivity.broken":
		cc.ConnBroken += increment
	case "ecn.connectivity.transient", "ecn.stable.connectivity.transient":
		cc.ConnTransient += increment
	case "ecn.connectivity.offline", "ecn.stable.connectivity.offline":
		cc.ConnOffline += increment
	case "ecn.connectivity.unstable", "ecn.stable.connectivity.unstable":
		cc.ConnUnstable += increment
	case "ecn.negotiation.succeeded", "ecn.stable.negotiation.succeeded", "ecn.negotiated":
		cc.NegoWorks += increment
	case "ecn.negotiation.failed", "ecn.stable.negotiation.failed", "ecn.not_negotiated":
		cc.NegoFailed += increment
	case "ecn.negotiation.reflected", "ecn.stable.negotiation.reflected":
		cc.NegoReflected += increment
	case "ecn.negotiation.unstable", "ecn.stable.negotiation.unstable":
		cc.NegoUnstable += increment
	case "ecn.ipmark.ect0.seen":
		cc.IpEct0 += increment
	case "ecn.ipmark.ect1.seen":
		cc.IpEct1 += increment
	case "ecn.ipmark.ce.seen":
		cc.IpCe += increment
	case "ecn.ipmark.ect0.not_seen":
		cc.NoIpEct0 += increment
	case "ecn.ipmark.ect1.not_seen":
		cc.NoIpEct1 += increment
	case "ecn.ipmark.ce.not_seen":
		cc.NoIpCe += increment
	default:
		cc.Total--
	}

}

func (cc *CondCount) Add(other *CondCount) {
	if cc.Total == 0 && other.Total != 0 {
		cc.TimeStart = other.TimeStart
		cc.TimeEnd = other.TimeEnd
	}

	if cc.TimeStart == nil || other.TimeStart != nil {
		cc.TimeStart = other.TimeStart
	}

	if cc.TimeEnd == nil || other.TimeEnd != nil {
		cc.TimeEnd = other.TimeEnd
	}

	if cc.TimeStart != nil && other.TimeStart != nil && cc.TimeStart.Sub(*other.TimeStart) > 0 {
		cc.TimeStart = other.TimeStart
	}

	if cc.TimeEnd != nil && other.TimeEnd != nil && other.TimeEnd.Sub(*cc.TimeEnd) > 0 {
		cc.TimeEnd = other.TimeEnd
	}

	cc.Total += other.Total

	cc.ConnWorks += other.ConnWorks
	cc.ConnBroken += other.ConnBroken
	cc.ConnTransient += other.ConnTransient
	cc.ConnOffline += other.ConnOffline
	cc.NegoWorks += other.NegoWorks
	cc.NegoFailed += other.NegoFailed
	cc.NegoReflected += other.NegoReflected
	cc.IpCe += other.IpCe
	cc.NoIpCe += other.NoIpCe
	cc.IpEct0 += other.IpEct0
	cc.NoIpEct0 += other.NoIpEct0
	cc.IpEct1 += other.IpEct1
	cc.NoIpEct1 += other.NoIpEct1
}
