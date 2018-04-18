package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

	"github.com/colinmarc/sequencefile"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var listFlag = flag.Bool("list", false, "list keys in file")
var extractFlag = flag.String("extract", "", "`key` to extract from file to stdout")

func decrapifySequenceKey(r rune) bool {
	return unicode.IsControl(r) || unicode.IsSpace(r)
}

func list(sf *sequencefile.Reader) error {
	for sf.Scan() {
		sfk := strings.TrimFunc(string(sf.Key()[:]), decrapifySequenceKey)
		fmt.Printf("%s\n", sfk)
	}
	return sf.Err()
}

func extract(sf *sequencefile.Reader, k string) error {
	for sf.Scan() {

		sfk := strings.TrimFunc(string(sf.Key()[:]), decrapifySequenceKey)
		log.Printf("comparing %#v == %#v", k, sfk)

		if k == sfk {
			log.Printf("extracting %s", sfk)

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

func extractAll(sf *sequencefile.Reader) error {
	for sf.Scan() {

		sfk := strings.TrimFunc(string(sf.Key()[:]), decrapifySequenceKey)

		if err := sf.Err(); err != nil {
			return err
		}

		log.Printf("extracting %s", sfk)

		out, err := os.Create(sfk)
		if err != nil {
			return err
		}

		if _, err := out.Write(sf.Value()); err != nil {
			return err
		}
	}

	return nil
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
		log.Printf("extracting everything from %s", args[0])
		if err := extractAll(sf); err != nil {
			log.Fatal(err)
		}
	}
}
