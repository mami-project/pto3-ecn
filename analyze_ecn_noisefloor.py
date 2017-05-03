import pandas as pd

class PathspiderECNSuperAnalyzer:
    """
    Create superconditions as an intermediate step
    toward analyzing path dependency of ECN connectivity
    """

    def __init__(self):
        pass

    def run(self, observations, writer):

        # Pull everything into a dataframe. (RAM is good. More RAM is better.)
        df = observations_to_dataframe(observations, ['ecn.connectivity.works',
                                                      'ecn.connectivity.broken',
                                                      'ecn.connectivity.transient',
                                                      'ecn.connectivity.offline'])

        # now mutate the DF to an indexed-by-destination count of conditions by condition,
        # expanding start_time and end_time to the full interval covered; output is sdf
        # FIXME not done yet
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

            writer.observe(start_time = target.start_time,
                           end_time =  target.end_time,
                           path = ['*', target.Index],
                           condition = condition,
                           value = None)

        writer.commit()

    def interested(self, conditions, metadata):
        if     'ecn.connectivity.works' not in conditions and \
               'ecn.connectivity.broken' not in conditions and \
               'ecn.connectivity.transient' not in conditions and \
               'ecn.connectivity.offline' not in conditions:
            return false

        return 'pathspider.ecn' in metadata

def ndjson_iterator(infile):
    """
    Iterate over an NDJSON file, producing lists for arrays and dicts for objects.

    """
    pass

def observations_to_dataframe(infile):
    pass

def condition_counts_by_target(df):
    pass
