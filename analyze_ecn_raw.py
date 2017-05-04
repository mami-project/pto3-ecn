import sys
import json
import argparse
import dateutil.parser

# FIXME Move me to mamipto.analyze, and make me less brittle

ISO8601_FMT = "%Y-%m-%dT%H:%M:%S"

class ObservationWriter:
    def __init__(self, outfile):
        self._outfile = outfile
        self._osid = 1

    def next_observation_set(self):
        self._osid += 1

    def observe(self, start_time, end_time, path, condition, value=None):
        rec = [self._osid,
               start_time.strftime(ISO8601_FMT),
               end_time.strftime(ISO8601_FMT),
               path, condition]

        if value is not None:
            rec.append(value)

        json.dump(rec, self._outfile)
        self._outfile.write("\n")

def interested(args):
    for ft in ['ps-ecn-ndjson', 'ps-ecn-ndjson-bz2']:
        if args.file_type == ft:
            return True
    return False

def analyze(args, infile, writer):
    for line in infile:
        raw_obs = json.loads(line)
        for condition in raw_obs['conditions']:
            writer.observe(start_time=dateutil.parser.parse(raw_obs['time']['from']),
                           end_time=dateutil.parser.parse(raw_obs['time']['to']),
                           path=[raw_obs['sip'], '*', raw_obs['dip']],
                           condition=condition)

def _parse_args():
    parser = argparse.ArgumentParser(description="Analyze Pathspider ECN plugin data into PTO observations")

    parser.add_argument("--interest", action="store_true", help="Evaluate interest instead of analyzing")
    parser.add_argument("--file-type", type=str, help="PTO file type")
    parser.add_argument("--time-start", type=str, help="Start time from raw file metadata")
    parser.add_argument("--time-end", type=str, help="End time from raw file metadata")
    parser.add_argument("-M", type=str, action="append", help="Additional metadata key=value pair")

    args = parser.parse_args()

    args.metadata = {}
    for kv in args.m:
        (k, v) = kv.split("=")
        if k is not None and v is not None:
            args.metadata[k] = v

    return args

if __name__ == "__main__":
    args = _parse_args()

    if args.interest:
        sys.exit(0 if interested(args) else 1)
    else:
        try:
            analyze(args, sys.stdin, ObservationWriter(sys.stdout))
            sys.exit(0)
        except Exception as e:
            sys.stderr.write(repr(e) + "\n")
            sys.exit(-1)