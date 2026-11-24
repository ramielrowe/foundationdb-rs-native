#### `get`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fresh_tx | 1 | native | 4 | 766 | 737-830 | 1.17 ms | 1.22 ms | 3.52 ms | 5.77 ms | 40.2 | 0.03 | 17 MB | 0 | 3 |
| fresh_tx | 1 | c | 4 | 6068 | 5386-6138 | 161 us | 176 us | 212 us | 276 us | 35.4 | 0.21 | 23 MB | 0 | 3 |
| fresh_tx | 16 | native | 4 | 12996 | 12433-13083 | 1.23 ms | 1.27 ms | 1.37 ms | 2.24 ms | 15.3 | 0.20 | 19 MB | 0 | 3 |
| fresh_tx | 16 | c | 4 | 86430 | 83410-86941 | 182 us | 210 us | 258 us | 387 us | 9.1 | 0.78 | 28 MB | 0 | 3 |
| fresh_tx | 256 | native | 4 | 148911 | 146204-150237 | 1.85 ms | 2.15 ms | 2.54 ms | 2.95 ms | 12.2 | 1.80 | 37 MB | 0 | 3 |
| fresh_tx | 256 | c | 4 | 177950 | 170458-179154 | 1.37 ms | 1.92 ms | 2.55 ms | 3.20 ms | 7.8 | 1.40 | 36 MB | 0 | 3 |

#### `set`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1_key_commit | 1 | native | 4 | 157 | 70.9-198 | 5.70 ms | 8.73 ms | 17.4 ms | 31.9 ms | 63.3 | 0.01 | 17 MB | 0 | 3 |
| 1_key_commit | 1 | c | 4 | 147 | 62.2-167 | 5.91 ms | 9.24 ms | 35.0 ms | 137.0 ms | 122.6 | 0.01 | 23 MB | 0 | 3 |
| 1_key_commit | 16 | native | 4 | 1558 | 956-1846 | 9.47 ms | 14.2 ms | 22.2 ms | 152.4 ms | 25.0 | 0.04 | 17 MB | 0 | 3 |
| 1_key_commit | 16 | c | 4 | 2000 | 1384-2617 | 6.16 ms | 11.5 ms | 41.7 ms | 97.4 ms | 16.4 | 0.03 | 24 MB | 0 | 3 |
| 1_key_commit | 256 | native | 4 | 20300 | 8995-20519 | 11.0 ms | 19.1 ms | 49.7 ms | 206.3 ms | 14.3 | 0.28 | 20 MB | 0 | 3 |
| 1_key_commit | 256 | c | 4 | 21201 | 19738-26283 | 9.68 ms | 14.9 ms | 35.9 ms | 265.8 ms | 9.3 | 0.19 | 29 MB | 0 | 3 |

#### `mixed`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 90r_10w_10ops | 1 | native | 4 | 425 | 415-475 | 1.51 ms | 5.24 ms | 10.1 ms | 46.0 ms | 204.4 | 0.09 | 17 MB | 0 | 3 |
| 90r_10w_10ops | 1 | c | 4 | 453 | 432-759 | 632 us | 6.11 ms | 13.1 ms | 40.6 ms | 251.3 | 0.17 | 23 MB | 0 | 3 |
| 90r_10w_10ops | 16 | native | 4 | 5946 | 5612-6395 | 1.66 ms | 3.25 ms | 14.7 ms | 32.9 ms | 147.6 | 0.86 | 19 MB | 0 | 3 |
| 90r_10w_10ops | 16 | c | 4 | 9167 | 6077-10740 | 648 us | 2.07 ms | 15.3 ms | 75.2 ms | 104.9 | 0.96 | 25 MB | 0 | 3 |
| 90r_10w_10ops | 256 | native | 4 | 29462 | 23896-29683 | 6.56 ms | 13.7 ms | 30.6 ms | 115.5 ms | 123.7 | 3.55 | 33 MB | 0 | 3 |
| 90r_10w_10ops | 256 | c | 4 | 22964 | 11876-24212 | 9.49 ms | 15.6 ms | 24.0 ms | 262.0 ms | 63.7 | 1.43 | 37 MB | 0 | 3 |

#### `many_tx`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3r_2w_commit | 1000 | native | 4 | 29955 | 28267-32296 | 25.5 ms | 48.9 ms | 163.4 ms | 481.4 ms | 62.4 | 1.92 | 74 MB | 0 | 3 |
| 3r_2w_commit | 1000 | c | 4 | 28602 | 26373-34603 | 26.3 ms | 48.2 ms | 202.2 ms | 297.0 ms | 34.4 | 0.99 | 51 MB | 0 | 3 |
| 3r_2w_commit | 5000 | native | 4 | 41426 | 37641-44405 | 89.7 ms | 198.3 ms | 521.3 ms | 775.5 ms | 76.6 | 3.17 | 265 MB | 0 | 3 |
| 3r_2w_commit | 5000 | c | 4 | 29371 | 29264-30511 | 107.3 ms | 306.4 ms | 715.3 ms | 987.0 ms | 44.0 | 1.30 | 142 MB | 0 | 3 |

#### Native relative to libfdb_c

Ratios of medians, native / libfdb_c, at the same application thread count. Throughput above 1.00 is native faster; latency and CPU below 1.00 is native better.

| workload | variant | conc | threads | ops/s | p50 | p99 | CPU/op |
|---|---|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 4 | 0.13 | 7.25 | 16.58 | 1.13 |
| get | fresh_tx | 16 | 4 | 0.15 | 6.75 | 5.33 | 1.69 |
| get | fresh_tx | 256 | 4 | 0.84 | 1.35 | 0.99 | 1.56 |
| set | 1_key_commit | 1 | 4 | 1.07 | 0.97 | 0.50 | 0.52 |
| set | 1_key_commit | 16 | 4 | 0.78 | 1.54 | 0.53 | 1.53 |
| set | 1_key_commit | 256 | 4 | 0.96 | 1.14 | 1.38 | 1.55 |
| mixed | 90r_10w_10ops | 1 | 4 | 0.94 | 2.38 | 0.77 | 0.81 |
| mixed | 90r_10w_10ops | 16 | 4 | 0.65 | 2.55 | 0.96 | 1.41 |
| mixed | 90r_10w_10ops | 256 | 4 | 1.28 | 0.69 | 1.27 | 1.94 |
| many_tx | 3r_2w_commit | 1000 | 4 | 1.05 | 0.97 | 0.81 | 1.81 |
| many_tx | 3r_2w_commit | 5000 | 4 | 1.41 | 0.84 | 0.73 | 1.74 |

