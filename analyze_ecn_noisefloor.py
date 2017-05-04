import sys
import json
import argparse
import dateutil.parser

import pandas as pd
import numpy as np

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

Observation = collections.namedtuple("Observation", 
                                     ['set_id', 'start_time', 'end_time', 
                                      'path', 'condition', 'value'])

def observation_reader(infile):
    for line in infile:
        a = json.loads(line)
        while len(a) < 6:
            a.append(None)
        yield Observation(*a)

def interested(args):
    return ('ecn.connectivity.works' in args.conditions or
            'ecn.connectivity.broken' in args.conditions or
            'ecn.connectivity.transient' in args.conditions or
            'ecn.connectivity.offline' in args.conditions)
        
def observations_to_dataframe(obs_iter, conditions):
    #FIXME write me
    return None

def condition_counts_by_target(df):
    #FIXME write me
    return None

def analyze(args, reader, writer):

    # haha nice RAM you got there, i'll take it!
    df = observations_to_dataframe(reader, ['ecn.connectivity.works',
                                            'ecn.connectivity.broken',
                                            'ecn.connectivity.transient',
                                            'ecn.connectivity.offline'])
    
    sdf = condition_counts_by_target(df)

    # calculate intermediate conditions
    sdf['int_e1seen'] = (sdf['ecn.connectivity.works'] + sdf['ecn.connectivity.transient']) > 0
    sdf['int_e0seen'] = (sdf['ecn.connectivity.works'] + sdf['ecn.connectivity.broken']) > 0

    # calculate super conditions
    sdf['super_works'] =         sdf['int_e0seen'] & sdf['int_e1seen']
    sdf['super_offline'] =      ~sdf['int_e0seen'] & ~sdf['int_e1seen']
    sdf['super_broken'] = (     (sdf['ecn.connectivity.broken'] > 0) &
                                (sdf['ecn.connectivity.works'] == 0) &
                                (sdf['ecn.connectivity.transient'] == 0))
    sdf['super_transient'] = (  (sdf['ecn.connectivity.transient'] > 0) &
                                (sdf['ecn.connectivity.works'] == 0) &
                                (sdf['ecn.connectivity.broken'] == 0))

    writer.begin()
    writer['pathspider.ecn.super'] = "yes"

    # and iterate

    for target in sdf.loc[:,['start_time','end_time',
                                'super_works','super_offline',
                                'super_broken','super_transient','super_weird']].itertuples():

        if target.super_works:
            condition = "ecn.connectivity.super.works"
        elif target.super_broken:
            condition = "ecn.connectivity.super.broken"
        elif target.super_transient:
            condition = "ecn.connectivity.super.transient"
        elif target.super_offline:
            condition = "ecn.connectivity.super.offline"
        elif target.super_offline:
            condition = "ecn.connectivity.super.weird"

        writer.observe(start_time=target.start_time,
                       end_time=target.end_time,
                       path=['*', target.Index],
                       condition=condition,
                       value=None)

def _parse_args():
    parser = argparse.ArgumentParser(description="Suppress noise in Pathspider ECN observations")

    parser.add_argument("--interest", action="store_true", help="Evaluate interest instead of analyzing")
    parser.add_argument("--condition", type=str, action="append", help="Condition present in observation set")
    parser.add_argument("--time-start", type=str, help="Observation set start time")
    parser.add_argument("--time-end", type=str, help="Observation set end time")
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
            analyze(args, observation_reader(sys.stdin), ObservationWriter(sys.stdout))
            sys.exit(0)
        except Exception as e:
            sys.stderr.write(repr(e) + "\n")
            sys.exit(-1)
