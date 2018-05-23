package main

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/calmh/ipfix"

	pto3 "github.com/mami-project/pto3-go"
)

func init() {
	var err error
	ieSpecRegexp, err = regexp.Compile(`^([^\s\[\<\(]+)?(\(((\d+)\/)?(\d+)\))?(\<(\S+)\>)?(\[(\S+)\])?`)
	if err != nil {
		panic(err)
	}

	doExtractField = map[string]struct{}{
		"initialTCPFlags":              struct{}{},
		"lastSynTcpFlags":              struct{}{},
		"reverseLastSynTcpFlags":       struct{}{},
		"reverseQofTcpCharacteristics": struct{}{},
		"flowStartMilliseconds":        struct{}{},
		"sourceTransportPort":          struct{}{},
		"destinationTransportPort":     struct{}{},
		"sourceIPv4Address":            struct{}{},
		"sourceIPv6Address":            struct{}{},
		"destinationIPv4Address":       struct{}{},
		"destinationIPv6Address":       struct{}{},
	}

}

var doExtractField map[string]struct{}

var ieSpecRegexp *regexp.Regexp

var qofSpecifiers = `initialTCPFlags(6871/14)<unsigned8>[1]
unionTCPFlags(6871/15)<unsigned8>[1]
reverseFlowDeltaMilliseconds(6871/21)<signed32>[4]
reverseInitialTCPFlags(6871/16398)<unsigned8>[1]
reverseUnionTCPFlags(6871/16399)<unsigned8>[1]
expiredFragmentCount(6871/100)<unsigned32>[4]
assembledFragmentCount(6871/101)<unsigned32>[4]
meanFlowRate(6871/102)<unsigned32>[4]
meanPacketRate(6871/103)<unsigned32>[4]
flowTableFlushEventCount(6871/104)<unsigned32>[4]
flowTablePeakCount(6871/105)<unsigned32>[4]
tcpSequenceCount(35566/1024)<unsigned64>[8]
tcpRetransmitCount(35566/1025)<unsigned64>[8]
maxTcpSequenceJump(35566/1026)<unsigned32>[4]
minTcpRttMilliseconds(35566/1029)<unsigned32>[4]
lastTcpRttMilliseconds(35566/1030)<unsigned32>[4]
ectMarkCount(35566/1031)<unsigned64>[8]
ceMarkCount(35566/1032)<unsigned64>[8]
declaredTcpMss(35566/1033)<unsigned16>[2]
observedTcpMss(35566/1034)<unsigned16>[2]
tcpSequenceLossCount(35566/1035)<unsigned64>[8]
tcpSequenceJumpCount(35566/1036)<unsigned64>[8]
tcpLossEventCount(35566/1038)<unsigned64>[8]
qofTcpCharacteristics(35566/1039)<unsigned32>[4]
tcpDupAckCount(35566/1040)<unsigned64>[8]
tcpSelAckCount(35566/1041)<unsigned64>[8]
minTcpRwin(35566/1042)<unsigned32>[4]
meanTcpRwin(35566/1043)<unsigned32>[4]
maxTcpRwin(35566/1044)<unsigned32>[4]
tcpReceiverStallCount(35566/1045)<unsigned32>[4]
tcpRttSampleCount(35566/1046)<unsigned32>[4]
tcpTimestampFrequency(35566/1047)<unsigned32>[4]
minTcpChirpMilliseconds(35566/1048)<signed16>[2]
maxTcpChirpMilliseconds(35566/1049)<signed16>[2]
minTcpIOTMilliseconds(35566/1050)<unsigned32>[4]
maxTcpIOTMilliseconds(35566/1051)<unsigned32>[4]
meanTcpChirpMilliseconds(35566/1052)<signed16>[2]
lastSynTcpFlags(35566/1053)<unsigned8>[1]
reverseTcpSequenceCount(35566/17408)<unsigned64>[8]
reverseTcpRetransmitCount(35566/17409)<unsigned64>[8]
reverseMaxTcpSequenceJump(35566/17410)<unsigned32>[4]
reverseEctMarkCount(35566/17415)<unsigned64>[8]
reverseCeMarkCount(35566/17416)<unsigned64>[8]
reverseDeclaredTcpMss(35566/17417)<unsigned16>[2]
reverseObservedTcpMss(35566/17418)<unsigned16>[2]
reverseTcpSequenceLossCount(35566/17419)<unsigned64>[8]
reverseTcpSequenceJumpCount(35566/17420)<unsigned64>[8]
reverseTcpLossEventCount(35566/17422)<unsigned64>[8]
reverseQofTcpCharacteristics(35566/17423)<unsigned32>[4]
reverseTcpDupAckCount(35566/17424)<unsigned64>[8]
reverseTcpSelAckCount(35566/17425)<unsigned64>[8]
reverseMinTcpRwin(35566/17426)<unsigned32>[4]
reverseMeanTcpRwin(35566/17427)<unsigned32>[4]
reverseMaxTcpRwin(35566/17428)<unsigned32>[4]
reverseTcpReceiverStallCount(35566/17429)<unsigned32>[4]
reverseTcpTimestampFrequency(35566/17431)<unsigned32>[4]
reverseMinTcpChirpMilliseconds(35566/17432)<signed16>[2]
reverseMaxTcpChirpMilliseconds(35566/17433)<signed16>[2]
reverseMinTcpIOTMilliseconds(35566/17434)<unsigned32>[4]
reverseMaxTcpIOTMilliseconds(35566/17435)<unsigned32>[4]
reverseMeanTcpChirpMilliseconds(35566/17436)<signed16>[2]
reverseLastSynTcpFlags(35566/17437)<unsigned8>[1]
`

func parseIESpec(spec string) (ipfix.DictionaryEntry, error) {

	m := ieSpecRegexp.FindStringSubmatch(spec)
	if m == nil {
		return ipfix.DictionaryEntry{}, fmt.Errorf("cannot parse iespec %s", spec)
	}

	pen, err := strconv.Atoi(m[4])
	if err != nil {
		pen = 0
	}
	pen32 := uint32(pen)

	ienum, err := strconv.Atoi(m[5])
	if err != nil {
		return ipfix.DictionaryEntry{}, fmt.Errorf("missing IE number in %s", spec)
	}
	ienum16 := uint16(ienum)

	ietype, ok := ipfix.FieldTypes[m[7]]
	if !ok {
		return ipfix.DictionaryEntry{}, fmt.Errorf("bad IE type in %s", spec)
	}

	return ipfix.DictionaryEntry{Name: m[1], FieldID: ienum16, EnterpriseID: pen32, Type: ietype}, nil
}

func qofSession() (*ipfix.Session, *ipfix.Interpreter) {

	s := ipfix.NewSession()
	i := ipfix.NewInterpreter(s)

	scanner := bufio.NewScanner(bytes.NewBuffer([]byte(qofSpecifiers)))

	for scanner.Scan() {
		dictEntry, err := parseIESpec(scanner.Text())
		if err != nil {
			panic(err)
		}
		i.AddDictionaryEntry(dictEntry)
	}

	return s, i
}

type QofObserver struct {
	out             io.Writer
	sourceOverride  string
	sourcePrepend   string
	requiredDstPort uint16

	hasCondition map[string]struct{}

	pendingTCPFlows map[string]*QofTCPFlow
	pendingECNFlows map[string]*QofTCPFlow

	sourceCounts          map[string]int
	sourceRejectThreshold int

	handledFlowCount int
	ignoredFlowCount int
}

func NewQofObserver() *QofObserver {
	out := new(QofObserver)

	out.hasCondition = make(map[string]struct{})
	out.pendingTCPFlows = make(map[string]*QofTCPFlow)
	out.pendingECNFlows = make(map[string]*QofTCPFlow)
	out.sourceCounts = make(map[string]int)
	out.sourceRejectThreshold = 100
	return out
}

const (
	FIN      = 0x01
	SYN      = 0x02
	RST      = 0x04
	PSH      = 0x08
	ACK      = 0x10
	URG      = 0x20
	ECE      = 0x40
	CWR      = 0x80
	QECT0    = 0x01
	QECT1    = 0x02
	QCE      = 0x04
	QTSOPT   = 0x10
	QSACKOPT = 0x20
	QWSOPT   = 0x40
	QSYNECT0 = 0x0100
	QSYNECT1 = 0x0200
	QSYNCE   = 0x0400
)

type QofTCPFlow struct {
	startTime     time.Time
	srcAddr       *net.IP
	dstAddr       *net.IP
	srcPort       uint16
	dstPort       uint16
	fwdLastSyn    uint8
	revLastSyn    uint8
	revQofChars   uint32
	didEstablish  bool
	ecnAttempted  bool
	ecnNegotiated bool
	ecnReflected  bool
	ecnECT0       bool
	ecnECT1       bool
	ecnCE         bool
}

func flowFromMap(fmap map[string]interface{}, requireSyn bool, requireDport uint16) (*QofTCPFlow, error) {

	// drop flows without syn
	if requireSyn {
		fif, ok := fmap["initialTCPFlags"]
		if !ok {
			return nil, fmt.Errorf("missing initial flags")
		}

		if fif.(uint8)&SYN == 0 {
			return nil, fmt.Errorf("no syn")
		}
	}

	// drop flows without required port
	dp, ok := fmap["destinationTransportPort"]
	if !ok {
		return nil, fmt.Errorf("missing destination port")
	}

	if requireDport > 0 && dp.(uint16) != requireDport {
		return nil, fmt.Errorf("bad destination port")
	}

	// get the rest of the required keys from the map
	stime, ok := fmap["flowStartMilliseconds"]
	if !ok {
		return nil, fmt.Errorf("missing start time")
	}

	sa, ok := fmap["sourceIPv4Address"]
	if !ok {
		sa, ok = fmap["sourceIPv6Address"]
		if !ok {
			return nil, fmt.Errorf("missing source IP address")
		}
	}

	da, ok := fmap["destinationIPv4Address"]
	if !ok {
		da, ok = fmap["destinationIPv6Address"]
		if !ok {
			return nil, fmt.Errorf("missing destination IP address")
		}
	}

	sp, ok := fmap["sourceTransportPort"]
	if !ok {
		return nil, fmt.Errorf("missing source port")
	}

	fls, ok := fmap["lastSynTcpFlags"]
	if !ok {
		return nil, fmt.Errorf("missing forward syn flags")
	}

	rls, ok := fmap["reverseLastSynTcpFlags"]
	if !ok {
		return nil, fmt.Errorf("missing reverse syn flags")
	}

	rqc, ok := fmap["reverseQofTcpCharacteristics"]
	if !ok {
		return nil, fmt.Errorf("missing magic qof stuff")
	}

	// make a new flow
	out := new(QofTCPFlow)

	// extract time, addresses and ports
	out.startTime = stime.(time.Time)
	out.srcAddr = sa.(*net.IP)
	out.dstAddr = da.(*net.IP)
	out.srcPort = sp.(uint16)
	out.dstPort = dp.(uint16)

	// extract TCP flags
	out.fwdLastSyn = fls.(uint8)
	out.revLastSyn = rls.(uint8)
	out.revQofChars = rqc.(uint32)

	// calculate characteristics
	out.ecnAttempted = out.fwdLastSyn&(SYN|ACK|ECE|CWR) == (SYN | ECE | CWR)
	out.ecnNegotiated = out.revLastSyn&(SYN|ACK|ECE|CWR) == (SYN | ACK | ECE)
	out.ecnReflected = out.revLastSyn&(SYN|ACK|ECE|CWR) == (SYN | ACK | ECE | CWR)
	out.ecnECT0 = out.revQofChars&QECT0 == QECT0
	out.ecnECT1 = out.revQofChars&QECT0 == QECT1
	out.ecnCE = out.revQofChars&QCE == QCE
	out.didEstablish = (out.fwdLastSyn&(SYN|ACK|FIN|RST) == (SYN) &&
		out.revLastSyn&(SYN|ACK|FIN|RST) == (SYN|ACK))

	return out, nil
}

func (qobs *QofObserver) observe(pathflow *QofTCPFlow, conditions ...string) error {

	// make a path
	path := new(pto3.Path)

	// handle source override from metadata
	var source string
	if qobs.sourceOverride != "" {
		source = qobs.sourceOverride
	} else {
		source = pathflow.srcAddr.String()
	}

	// handle source prepend from metadata
	var pathElements []string
	if qobs.sourcePrepend != "" {
		if source != "" {
			pathElements = []string{qobs.sourcePrepend, source, "*", pathflow.dstAddr.String()}
		} else {
			pathElements = []string{qobs.sourcePrepend, "*", pathflow.dstAddr.String()}
		}
	} else {
		if source != "" {
			pathElements = []string{source, "*", pathflow.dstAddr.String()}
		} else {
			pathElements = []string{"*", pathflow.dstAddr.String()}
		}
	}

	path.String = strings.Join(pathElements, " ")

	obsen := make([]pto3.Observation, len(conditions))
	for i, c := range conditions {
		obsen[i].TimeStart = &pathflow.startTime
		obsen[i].TimeEnd = &pathflow.startTime
		obsen[i].Path = path
		obsen[i].Condition = new(pto3.Condition)
		obsen[i].Condition.Name = c

		qobs.hasCondition[c] = struct{}{}
	}

	return pto3.WriteObservations(obsen, qobs.out)
}

func (qobs *QofObserver) matchFlows(flowkey string, tcpflow, ecnflow *QofTCPFlow) error {

	// generate connectivity condition
	var connectivityCondition string
	if tcpflow.didEstablish && ecnflow.didEstablish {
		connectivityCondition = "ecn.connectivity.works"
	} else if tcpflow.didEstablish && !ecnflow.didEstablish {
		connectivityCondition = "ecn.connectivity.broken"
	} else if !tcpflow.didEstablish && ecnflow.didEstablish {
		connectivityCondition = "ecn.connectivity.transient"
	} else if !tcpflow.didEstablish && !ecnflow.didEstablish {
		connectivityCondition = "ecn.connectivity.offline"
	}

	// generate negotiation condition
	var negotiationCondition string
	if ecnflow.ecnNegotiated {
		negotiationCondition = "ecn.negotiation.succeeded"
	} else if ecnflow.ecnReflected {
		negotiationCondition = "ecn.negotiation.reflected"
	} else {
		negotiationCondition = "ecn.negotiation.failed"
	}

	// generate mark conditions
	var ect0Condition, ect1Condition, ceCondition string
	if ecnflow.ecnECT0 {
		ect0Condition = "ecn.ipmark.ect0.seen"
	} else {
		ect0Condition = "ecn.ipmark.ect0.not_seen"
	}

	if ecnflow.ecnECT1 {
		ect1Condition = "ecn.ipmark.ect1.seen"
	} else {
		ect1Condition = "ecn.ipmark.ect1.not_seen"
	}

	if ecnflow.ecnCE {
		ceCondition = "ecn.ipmark.ce.seen"
	} else {
		ceCondition = "ecn.ipmark.ce.not_seen"
	}

	if err := qobs.observe(ecnflow, connectivityCondition, negotiationCondition, ect0Condition, ect1Condition, ceCondition); err != nil {
		return err
	}

	// match is no longer pending
	delete(qobs.pendingECNFlows, flowkey)
	delete(qobs.pendingTCPFlows, flowkey)

	return nil
}

func (qobs *QofObserver) handleFlow(fmap map[string]interface{}) error {

	// turn the map into a flow, skip flows we don't care about
	flow, err := flowFromMap(fmap, true, qobs.requiredDstPort)
	if err != nil {
		qobs.ignoredFlowCount++
		return err
	}

	// extract addresses, reject reversed flows
	source := flow.srcAddr.String()
	qobs.sourceCounts[source]++

	flowkey := flow.dstAddr.String()

	if qobs.handledFlowCount > qobs.sourceRejectThreshold &&
		qobs.sourceCounts[flowkey] > qobs.sourceRejectThreshold/2 {
		qobs.ignoredFlowCount++
		return errors.New("ignoring reversed flow")
	}

	qobs.handledFlowCount++

	// determine whether the flow is an ECN attempt or not
	if flow.ecnAttempted {
		ecnflow := flow
		if tcpflow, ok := qobs.pendingTCPFlows[flowkey]; ok {
			return qobs.matchFlows(flowkey, tcpflow, ecnflow)
		} else {
			qobs.pendingECNFlows[flowkey] = ecnflow
		}
	} else {
		tcpflow := flow
		if ecnflow, ok := qobs.pendingECNFlows[flowkey]; ok {
			return qobs.matchFlows(flowkey, tcpflow, ecnflow)
		} else {
			qobs.pendingTCPFlows[flowkey] = tcpflow
		}
	}

	return nil
}

func normalizeQoF(in io.Reader, metain io.Reader, out io.Writer) error {
	// unmarshal metadata into an RDS metadata object
	md, err := pto3.RawMetadataFromReader(metain, nil)
	if err != nil {
		return fmt.Errorf("could not read metadata: %s", err.Error())
	}

	var r io.Reader
	s := ipfix.NewSession()

	switch md.Filetype(true) {
	case "ecnspider-qof-ipfix":
		r = in
	case "ecnspider-qof-ipfix-bz2":
		r = bzip2.NewReader(in)
	default:
		return fmt.Errorf("unsupported filetype %s", md.Filetype(true))
	}

	// create an extractor around the output stream and initialize it with metadata
	qobs := NewQofObserver()
	qobs.out = out
	qobs.sourceOverride = md.Get("source_override", true)
	qobs.sourcePrepend = md.Get("source_prepend", true)
	dstPort64, _ := strconv.ParseUint(md.Get("dst_port", true), 10, 16)
	qobs.requiredDstPort = uint16(dstPort64)

	// get IPFIX session and intepreter
	s, i := qofSession()

	// now iterate over IPFIX messages
	for {
		msg, err := s.ParseReader(r)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		// interpret records in each message into a map,
		// then pass them to the condition extractor
		for _, rec := range msg.DataRecords {

			fields := i.Interpret(rec)

			// let's do this slooooooowly.....
			mrec := make(map[string]interface{})
			for _, field := range fields {
				if _, ok := doExtractField[field.Name]; ok {
					mrec[field.Name] = field.Value
				}
			}

			qobs.handleFlow(mrec)
			if qobs.handledFlowCount%1000 == 0 {
				log.Printf("ignored %d handled %d pending TCP %d pending ECN %d\n",
					qobs.ignoredFlowCount,
					qobs.handledFlowCount,
					len(qobs.pendingTCPFlows),
					len(qobs.pendingECNFlows))
			}

		}
	}

	// dump pending flows?
	log.Printf("%d pending TCP flows, %d pending ECN flows", len(qobs.pendingTCPFlows), len(qobs.pendingECNFlows))

	// now write metadata
	mdout := make(map[string]interface{})
	mdcond := make([]string, 0)

	// copy all aux metadata from the raw file
	for k := range md.Metadata {
		mdout[k] = md.Metadata[k]
	}

	// create condition list from observed conditions
	for k := range qobs.hasCondition {
		mdcond = append(mdcond, k)
	}
	mdout["_conditions"] = mdcond

	// add start and end time and owner, since we have it
	mdout["_owner"] = md.Owner(true)
	mdout["_time_start"] = md.TimeStart(true).Format(time.RFC3339)
	mdout["_time_end"] = md.TimeEnd(true).Format(time.RFC3339)

	// hardcode analyzer path (FIXME, tag?)
	mdout["_analyzer"] = "https://raw.githubusercontent.com/mami-project/pto3-ecn/master/ecn_qof_normalizer/ecn_qof_normalizer.json"

	// serialize and write to stdout
	b, err := json.Marshal(mdout)
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %s", err.Error())
	}

	if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
		return fmt.Errorf("error writing metadata: %s", err.Error())
	}

	// all done yay
	return nil
}

func main() {
	// force UTC (only works on Unix)
	os.Setenv("TZ", "")

	// wrap a file around the metadata stream
	mdfile := os.NewFile(3, ".piped_metadata.json")

	// and go
	if err := normalizeQoF(os.Stdin, mdfile, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
