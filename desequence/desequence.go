package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/colinmarc/sequencefile"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var listFlag = flag.Bool("list", false, "list keys in file")
var extractFlag = flag.String("extract", "", "`key` to extract from file to stdout")

func list(sf *sequencefile.Reader) error {
	for sf.Scan() {
		fmt.Printf("%s\n", sf.Key())
	}
	return sf.Err()
}

func extract(sf *sequencefile.Reader, k string) error {
	for sf.Scan() {

		log.Printf("comparing %#v == %#v", k, strings.TrimSpace(string(sf.Key()[:])))

		if k == strings.TrimSpace(string(sf.Key()[:])) {
			out, err := os.Create(k)
			if err != nil {
				return err
			}
			_, err = out.Write(sf.Value())
			return err
		}
	}
	if err := sf.Err(); err != nil {
		return err
	}
	return fmt.Errorf("missing key %s; use -list to list keys", k)
}

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: list or extract entries from a sequence file\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s [-list|-extract key] file.seq\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "must supply exactly one sequence file")
		flag.Usage()
		os.Exit(1)
	}

	sf, err := sequencefile.Open(args[0])
	if err != nil {
		log.Fatal(err)
	}

	if *listFlag {
		if err := list(sf); err != nil {
			log.Fatal(err)
		}
	} else if *extractFlag != "" {
		if err := extract(sf, *extractFlag); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "missing -list or -extract flag")
		flag.Usage()
		os.Exit(1)
	}
}
