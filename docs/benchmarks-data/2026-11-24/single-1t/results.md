#### `get`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fresh_tx | 1 | native | 1 | 852 | 656-870 | 1.12 ms | 1.16 ms | 2.21 ms | 3.76 ms | 32.8 | 0.03 | 17 MB | 0 | 3 |
| fresh_tx | 1 | c | 1 | 9171 | 8896-10115 | 101 us | 119 us | 153 us | 292 us | 30.3 | 0.28 | 23 MB | 0 | 3 |
| fresh_tx | 16 | native | 1 | 12619 | 12378-12806 | 1.24 ms | 1.29 ms | 2.26 ms | 2.34 ms | 8.0 | 0.10 | 18 MB | 0 | 3 |
| fresh_tx | 16 | c | 1 | 97999 | 65808-106518 | 142 us | 207 us | 347 us | 1.78 ms | 6.9 | 0.68 | 26 MB | 0 | 3 |
| fresh_tx | 256 | native | 1 | 110414 | 109192-119139 | 2.15 ms | 3.10 ms | 3.61 ms | 4.22 ms | 7.7 | 0.85 | 31 MB | 0 | 3 |
| fresh_tx | 256 | c | 1 | 166329 | 144132-175870 | 1.44 ms | 2.02 ms | 2.51 ms | 3.16 ms | 6.2 | 1.01 | 34 MB | 0 | 3 |

#### `get_seq`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100_per_tx | 1 | native | 1 | 25029 | 23831-26229 | 27 us | 30 us | 43 us | 68 us | 11.1 | 0.28 | 17 MB | 0 | 3 |
| 100_per_tx | 1 | c | 1 | 24190 | 17768-24846 | 38 us | 44 us | 54 us | 85 us | 14.6 | 0.35 | 23 MB | 0 | 3 |
| 100_per_tx | 16 | native | 1 | 108157 | 102910-118487 | 135 us | 152 us | 241 us | 414 us | 6.6 | 0.72 | 23 MB | 0 | 3 |
| 100_per_tx | 16 | c | 1 | 218713 | 202863-238944 | 65 us | 92 us | 132 us | 224 us | 5.5 | 1.18 | 33 MB | 0 | 3 |
| 100_per_tx | 256 | native | 1 | 145588 | 142548-148881 | 1.56 ms | 1.85 ms | 3.02 ms | 4.60 ms | 6.9 | 1.00 | 46 MB | 0 | 3 |
| 100_per_tx | 256 | c | 1 | 243854 | 231083-253408 | 944 us | 1.25 ms | 1.98 ms | 2.75 ms | 5.2 | 1.32 | 60 MB | 0 | 3 |

#### `mixed`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 90r_10w_10ops | 1 | native | 1 | 490 | 475-567 | 1.36 ms | 4.71 ms | 7.60 ms | 10.5 ms | 116.2 | 0.07 | 17 MB | 0 | 3 |
| 90r_10w_10ops | 1 | c | 1 | 879 | 875-1140 | 491 us | 3.12 ms | 8.08 ms | 16.3 ms | 204.1 | 0.18 | 23 MB | 0 | 3 |
| 90r_10w_10ops | 16 | native | 1 | 5688 | 5217-5755 | 2.29 ms | 3.82 ms | 12.2 ms | 23.0 ms | 67.1 | 0.38 | 18 MB | 0 | 3 |
| 90r_10w_10ops | 16 | c | 1 | 7459 | 5962-10033 | 859 us | 5.51 ms | 13.4 ms | 26.0 ms | 80.7 | 0.60 | 23 MB | 0 | 3 |
| 90r_10w_10ops | 256 | native | 1 | 15806 | 12615-16047 | 15.8 ms | 17.2 ms | 22.0 ms | 210.6 ms | 62.2 | 0.98 | 29 MB | 0 | 3 |
| 90r_10w_10ops | 256 | c | 1 | 22948 | 11587-26498 | 9.82 ms | 14.1 ms | 20.9 ms | 81.2 ms | 54.1 | 1.24 | 34 MB | 0 | 3 |

#### `many_tx`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3r_2w_commit | 1000 | native | 1 | 28667 | 19208-29155 | 30.3 ms | 48.2 ms | 137.4 ms | 236.1 ms | 31.0 | 0.89 | 64 MB | 0 | 3 |
| 3r_2w_commit | 1000 | c | 1 | 30340 | 20299-35762 | 25.3 ms | 54.0 ms | 149.9 ms | 307.3 ms | 30.3 | 0.81 | 48 MB | 0 | 3 |
| 3r_2w_commit | 5000 | native | 1 | 20037 | 9878-20206 | 197.4 ms | 467.6 ms | 886.1 ms | 1321.7 ms | 47.9 | 0.96 | 245 MB | 0 | 3 |
| 3r_2w_commit | 5000 | c | 1 | 20597 | 18163-28363 | 156.4 ms | 424.0 ms | 900.5 ms | 1548.0 ms | 42.7 | 0.97 | 137 MB | 0 | 3 |

#### Native relative to libfdb_c

Ratios of medians, native / libfdb_c, at the same application thread count. Throughput above 1.00 is native faster; latency and CPU below 1.00 is native better.

| workload | variant | conc | threads | ops/s | p50 | p99 | CPU/op |
|---|---|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 1 | 0.09 | 11.09 | 14.46 | 1.08 |
| get | fresh_tx | 16 | 1 | 0.13 | 8.71 | 6.51 | 1.16 |
| get | fresh_tx | 256 | 1 | 0.66 | 1.49 | 1.44 | 1.25 |
| get_seq | 100_per_tx | 1 | 1 | 1.03 | 0.71 | 0.80 | 0.76 |
| get_seq | 100_per_tx | 16 | 1 | 0.49 | 2.08 | 1.83 | 1.19 |
| get_seq | 100_per_tx | 256 | 1 | 0.60 | 1.66 | 1.53 | 1.31 |
| mixed | 90r_10w_10ops | 1 | 1 | 0.56 | 2.78 | 0.94 | 0.57 |
| mixed | 90r_10w_10ops | 16 | 1 | 0.76 | 2.67 | 0.91 | 0.83 |
| mixed | 90r_10w_10ops | 256 | 1 | 0.69 | 1.61 | 1.05 | 1.15 |
| many_tx | 3r_2w_commit | 1000 | 1 | 0.94 | 1.20 | 0.92 | 1.02 |
| many_tx | 3r_2w_commit | 5000 | 1 | 0.97 | 1.26 | 0.98 | 1.12 |

