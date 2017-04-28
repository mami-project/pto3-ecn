import sys
import json
import dateutil.parser

# FIXME Move me to mamipto.analyze, and make me less brittle

class ObservationWriter:
    def __init__(self, outfile):
        self._outfile = outfile
        self._osid = 1

    def next_observation_set(self):
        self._osid += 1

    def observe(self, start_time, end_time, path, condition, value=None):
        rec = [self._osid, start_time, end_time, path, condition]

        if value is not None:
            rec.append(value)

        json.dump(rec, self._outfile)
        self._outfile.write("\n".encode())


def interested_in(metadata):
    return (metadata['file_type'] == 'ps-ecn-ndjson' or
            metadata['file_type'] == 'ps-ecn-ndjson-bz2')

def analyze(infile, writer):
    for line in infile:
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

