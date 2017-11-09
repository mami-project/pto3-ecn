
class PathspiderECNPathDepAnalyzer:
    def run(self, observations, writer):
        # Pull everything into a dataframe. (RAM is good. More RAM is better.)
        df = observations_to_dataframe(observations, ['ecn.connectivity.super.works',
                                                      'ecn.connectivity.super.broken',
                                                      'ecn.connectivity.super.transient',
                                                      'ecn.connectivity.super.offline',
                                                      'ecn.connectivity.super.weird'])

        sdf = condition_counts_by_target(df)

        sdf['path_dep'] = ( (sdf['ecn.connectivity.super.broken'] >= 1) &
                            (sdf['ecn.connectivity.super.works'] >= 1) &
                            (sdf['ecn.connectivity.super.offline'] == 0) &
                            (sdf['ecn.connectivity.super.transient'] == 0) &
                            (sdf['ecn.connectivity.super.weird'] == 0))

        sdf['site_dep'] = ( (sdf['ecn.connectivity.super.broken'] >= 1) &
                            (sdf['ecn.connectivity.super.works'] == 0) &
                            (sdf['ecn.connectivity.super.offline'] == 0) &
                            (sdf['ecn.connectivity.super.transient'] == 0) &
                            (sdf['ecn.connectivity.super.weird'] == 0))

        writer.begin()
        writer['pathspider.ecn.dependency'] = 'yes'

        for target in sdf.loc[:,['start_time','end_time',
                                 'path_dep', 'site_dep']].itertuples():

            if target.path_dep:
                condition = "ecn.connectivity.path_dependent"
            elif target.site_dep:
                condition = "ecn.connectivity.site_dependent"
            else
                condition = None

            if condition is not None:
                writer.observe(start_time = target.start_time,
                               end_time =  target.end_time,
                               path = ['*', target.Index],
                               condition = condition,
                               value = None)

    def interested(self, conditions, metadata):
        if     'ecn.connectivity.super.works' not in conditions and \
               'ecn.connectivity.super.broken' not in conditions and \
               'ecn.connectivity.super.transient' not in conditions and \
               'ecn.connectivity.super.offline' not in conditions and \
               'ecn.connectivity.super.weird' not in conditions
            return false

        return 'pathspider.ecn.super' in metadata
 
