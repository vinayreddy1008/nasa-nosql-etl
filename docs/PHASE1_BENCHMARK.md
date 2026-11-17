# Phase 1 Runtime Benchmark

Batch size: 100,000 records

Execution environment: single-machine Docker; Hadoop local JobRunner.

| Pipeline | Run ID | Runtime (s) |
|---|---|---:|
| mongo | `20261117T024856Z_10747678` | 97.734 |
| mongo | `20261117T035116Z_88e76064` | 80.298 |
| mongo | `20261117T035237Z_09ff3bbd` | 79.310 |

**mongo**
- Mean: 85.781 s
- Median: 80.298 s
- Sample standard deviation: 10.363 s

| mapreduce | `20261117T032602Z_9b1c5938` | 30.855 |
| mapreduce | `20261117T035043Z_3908a783` | 32.742 |
| mapreduce | `20261117T035356Z_8eb95601` | 30.689 |

**mapreduce**
- Mean: 31.429 s
- Median: 30.855 s
- Sample standard deviation: 1.140 s

## Limitations

These are observed local-system runtimes. Cache effects, background processes, and container resources may affect the comparison. Results are not representative of a multi-node cluster.
