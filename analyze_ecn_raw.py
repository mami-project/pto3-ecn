import sys
import json

from mamipto.analyze import ObservationWriter

def interested_in(metadata):
    return (metadata['format'] == 'ps-ecn-ndjson' or
            metadata['format'] == 'ps-ecn-ndjson-bz2')

def analyze(infile, outobs):
    for line in reader:
        raw_obs = json.loads(line)
        for condition in raw_obs['conditions']:
            writer.observe(start_time=dateutil.parser.parse(raw_obs['time']['from']),
                            end_time=dateutil.parser.parse(raw_obs['time']['to']),
                            path=[raw_obs['sip'], '*', raw_obs['dip']],
                            condition=condition)

if __name__ == "__main__":
    if "--pto-interest" in sys.argv:
        try:
            if interested_in(json.load(sys.stdin)):
                sys.exit(0)
            else:
                sys.exit(1)
        except Exception as e:
            sys.stderr.write((e.message + "\n").encode())
            sys.exit(-1)
    else:
        try:
            analyze(sys.stdin, ObservationWriter(sys.stdout))
            sys.exit(0)
        except Exception as e:
            sys.stderr.write((e.message + "\n").encode())
            sys.exit(-1)
            
        