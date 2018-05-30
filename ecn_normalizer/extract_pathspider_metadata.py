#!/usr/bin/env python3

import argparse
import json
import bz2
import dateutil.parser
import sys

def parse_args():
    ap = argparse.ArgumentParser(description="Given a list of input data files, create appropriate PTOv3 metadata files.")
    ap.add_argument("files", nargs="*", help="list of input files, compressed or not")
    ap.add_argument("-t", "--filetype", help="filetype [ps-ndjson]", default="ps-ndjson")
    return ap.parse_args()

def metadata_from_ps_ndjson(fp):
    y = None
    z = None

    for line in fp:
        d = json.loads(line)

        a = dateutil.parser.parse(d['time']['from'])
        b = dateutil.parser.parse(d['time']['to'])

        if y is None or a < y:
            y = a
            # print("y is now: "+y.strftime("%Y-%m-%dT%H:%M:%S"))
        
        if z is None or b > z:
            z = b    
            # print("z is now: "+z.strftime("%Y-%m-%dT%H:%M:%S"))

    return {'_time_start': y.strftime("%Y-%m-%dT%H:%M:%SZ"),
            '_time_end': z.strftime("%Y-%m-%dT%H:%M:%SZ")}

def write_metadata_for(filename, metadata_fn):
    metafilename = filename + ".meta.json"

    if filename.endswith(".bz2"):
        open_fn = bz2.open
    else:
        open_fn = open
    
    with open_fn(filename) as fp:
        metadata = metadata_fn(fp)
    
    with open(metafilename, mode="w") as mfp:
        json.dump(metadata, mfp, indent=2)

FILETYPE_MAP = { 'ps-ndjson': metadata_from_ps_ndjson }

if __name__ == "__main__":
    args = parse_args()

    for filename in args.files:
        print('processing %s...' % (filename,), end='')
        sys.stdout.flush()
        write_metadata_for(filename, FILETYPE_MAP[args.filetype])
        print('done.')
