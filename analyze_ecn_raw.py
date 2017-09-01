import sys
import json
import argparse
import dateutil.parser

from mamipto.analysis import ObservationWriter, parse_analyzer_args

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

if __name__ == "__main__":
    args = parse_analyzer_args("Analyze Pathspider ECN plugin data into PTO observations")

    if args.interest:
        sys.exit(0 if interested(args) else 1)
    else:
        try:
            analyze(args, sys.stdin, ObservationWriter(sys.stdout))
            sys.exit(0)
        except Exception as e:
            sys.stderr.write(repr(e) + "\n")
            sys.exit(-1)