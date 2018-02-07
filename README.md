# pto3-ecn

Normalizers and analyzers to convert ECNSpider (old) and
[PathSpider](https://pathspider.net) ECN connectivity and negotiation
measurement data to observations for the [MAMI](https://mami-project.eu) [Path
Transparency Observatory](https://github.com/mami-project/pto3-go) (PTO).

## ecn_normalizer

`ecn_normalizer` converts various raw data formats containing ECN to
observation files suitable for use with the PTO. It implements the PTO [local
normalizer
interface](https://github.com/mami-project/pto3-go/blob/master/doc/ANALYZER.md).

To use `ecn_normalizer` from the command-line (assuming bash or a bash-like shell):

```
$ ecn_normalizer < raw_data.ext 3< metadata.json > observations.ndjson
```

To use `ecn_normalizer` as a normalizer with a PTO instance, subsequently
loading the results into the database:

```
$ ptonorm -config pto_config.json ecn_normalizer campaign_name file_name > observations.ndjson
$ ptoload -config pto_config.json observations.ndjson
```

`ecn_normalizer` can handle raw data of the following filetypes: 

| Filetype                       | Description                                     |
| ------------------------------ | ----------------------------------------------- |
| `pathspider-v1-ecn-ndjson `    | Output from Pathspider v1 `ecn` plugin          |
| `pathspider-v1-ecn-ndjzon-bz2` | (compressed)                                    |

### Conditions

`ecn_normalizer` passes observations through from PathSpider; the conditions
generated and their meanings are described in the [ECN plugin
documentation](http://pathspider.readthedocs.io/en/latest/plugins/ecn.html)

### Additional Metadata 

`ecn_normalizer` passes any arbitrary metadata in the raw metadata through to
the observation metadata. In addition, it uses the following metadata keys for
its operation:

| Key               | Description                                                      |
| ----------------- | ---------------------------------------------------------------- |
| `source_override` | If present, replace first element in the path with this value    |
| `source_prepend`  | If present, insert value before first element in the path        |

## ecn_stabilizer

`ecn_stabilizer` looks at multiple measurements grouped by vantage point to
the same targets, and creates derived conditions for stable observed
connectivity. This is used to reduce the noise floor of transience in
connectivity observations. It implements the PTO [local analyzer
interface](https://github.com/mami-project/pto3-go/blob/master/doc/ANALYZER.md).

To use `ecn_stabilizer` with a PTO instance:

```
$ ptocat -config pto_config.json set_id ... | ecn_stabilizer > observations.ndjson
$ ptoload -config pto_config.json observations.ndjson
```