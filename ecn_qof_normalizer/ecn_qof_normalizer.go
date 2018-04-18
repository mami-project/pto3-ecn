package main

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"

	"github.com/calmh/ipfix"

	pto3 "github.com/mami-project/pto3-go"
)

func init() {
	var err error
	ieSpecRegexp, err = regexp.Compile(`^([^\s\[\<\(]+)?(\(((\d+)\/)?(\d+)\))?(\<(\S+)\>)?(\[(\S+)\])?`)
	if err != nil {
		panic(err)
	}

	doExtractField["destinationTransportPort"] = struct{}{}
	doExtractField["initialTCPFlags"] = struct{}{}

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

	pen, err := strconv.Atoi(m[2])
	if err != nil {
		pen = 0
	}
	pen32 := uint32(pen)

	ienum, err := strconv.Atoi(m[3])
	if err != nil {
		return ipfix.DictionaryEntry{}, fmt.Errorf("missing IE number in %s", spec)
	}
	ienum16 := uint16(ienum)

	var ietype ipfix.FieldType
	switch m[4] {
	case "unsigned8":
		ietype = ipfix.Uint8
	case "unsigned16":
		ietype = ipfix.Uint16
	case "unsigned32":
		ietype = ipfix.Uint32
	case "unsigned64":
		ietype = ipfix.Uint64
	case "signed8":
		ietype = ipfix.Int8
	case "signed16":
		ietype = ipfix.Int16
	case "signed32":
		ietype = ipfix.Int32
	case "signed64":
		ietype = ipfix.Int64
	case "float32":
		ietype = ipfix.Float32
	case "float64":
		ietype = ipfix.Float64
	case "boolean":
		ietype = ipfix.Boolean
	case "macAddress":
		ietype = ipfix.MacAddress
	case "octetArray":
		ietype = ipfix.OctetArray
	case "string":
		ietype = ipfix.String
	case "dateTimeSeconds":
		ietype = ipfix.DateTimeSeconds
	case "dateTimeMilliseconds":
		ietype = ipfix.DateTimeMilliseconds
	case "dateTimeMicroseconds":
		ietype = ipfix.DateTimeMicroseconds
	case "dateTimeNanoseconds":
		ietype = ipfix.DateTimeNanoseconds
	case "ipv4Address":
		ietype = ipfix.Ipv4Address
	case "ipv6Address":
		ietype = ipfix.Ipv6Address
	case "":
		ietype = ipfix.OctetArray
	default:
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

type qofConditionExtractor struct {
	out            io.Writer
	sourceOverride string
	sourcePrepend  string
	dstPort        uint16
}

const (
	FIN = 0x01
	SYN = 0x02
	RST = 0x04
	PSH = 0x08
	ACK = 0x10
	URG = 0x20
	ECE = 0x40
	CWR = 0x80
)

func (qce *qofConditionExtractor) handleFlow(flow map[string]interface{}) error {

	// drop all flows not on the destination port we care about
	if qce.dstPort != 0 && qce.dstPort != flow["destinationTransportPort"].(uint16) {
		return nil
	}

	// drop all flows without an initial SYN, since we joined them in the middle
	if flow["initialTCPFlags"].(uint8)&TcpSyn == 0 {
		return nil
	}

	// extract TCP flags
	fwdLastSyn := flow["lastSynTcpFlags"].(uint8)
	revLastSyn := flow["reverseLastSynTcpFlags"].(uint8)
	revQofChars := flow["reverseQofTcpCharacteristics"].(uint32)

	// calculate characteristics
	ecnAttempted := fwdLastSyn&(SYN|ACK|ECE|CWR) == (SYN | ECE | CWR)
	ecnNegotiated := revLastSyn&(SYN|ACK|ECE|CWR) == (SYN | ACK | ECE)

	// WORK POINTER
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
	qce := new(qofConditionExtractor)
	qce.out = out
	qce.sourceOverride = md.Get("source_override", true)
	qce.sourcePrepend = md.Get("source_prepend", true)
	dstPort64, _ := strconv.ParseUint(md.Get("dst_port", true), 10, 16)
	qce.dstPort = uint16(dstPort64)

	// get IPFIX session and intepreter
	s, i := qofSession()

	// now iterate over IPFIX messages
	for {
		msg, err := s.ParseReader(r)
		if err != nil {
			return err
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
			qce.handleFlow(mrec)
		}
	}

	// all done yay
	return nil
}

func main() {
	// wrap a file around the metadata stream
	mdfile := os.NewFile(3, ".piped_metadata.json")

	// and go
	if err := normalizeQoF(os.Stdin, mdfile, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
