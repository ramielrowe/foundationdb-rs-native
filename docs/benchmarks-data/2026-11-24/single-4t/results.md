#### `get`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fresh_tx | 1 | native | 4 | 883 | 834-888 | 1.12 ms | 1.14 ms | 2.12 ms | 2.16 ms | 32.0 | 0.03 | 17 MB | 0 | 3 |
| fresh_tx | 1 | c | 4 | 9877 | 9712-10063 | 98 us | 110 us | 129 us | 190 us | 29.9 | 0.30 | 23 MB | 0 | 3 |
| fresh_tx | 16 | native | 4 | 13347 | 13139-13437 | 1.19 ms | 1.22 ms | 1.29 ms | 2.25 ms | 12.5 | 0.17 | 18 MB | 0 | 3 |
| fresh_tx | 16 | c | 4 | 110285 | 92717-110735 | 142 us | 157 us | 201 us | 266 us | 7.4 | 0.80 | 27 MB | 0 | 3 |
| fresh_tx | 256 | native | 4 | 143793 | 143727-145369 | 1.88 ms | 2.16 ms | 2.49 ms | 2.92 ms | 11.1 | 1.60 | 35 MB | 0 | 3 |
| fresh_tx | 256 | c | 4 | 176072 | 164263-179927 | 1.48 ms | 1.59 ms | 2.04 ms | 2.82 ms | 6.6 | 1.19 | 37 MB | 0 | 3 |

#### `get_seq`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100_per_tx | 1 | native | 4 | 23980 | 23532-24821 | 28 us | 31 us | 44 us | 65 us | 14.2 | 0.34 | 17 MB | 0 | 3 |
| 100_per_tx | 1 | c | 4 | 24547 | 24536-24846 | 38 us | 41 us | 48 us | 71 us | 15.3 | 0.38 | 24 MB | 0 | 3 |
| 100_per_tx | 16 | native | 4 | 261464 | 260090-279578 | 50 us | 68 us | 123 us | 207 us | 10.7 | 2.81 | 29 MB | 0 | 3 |
| 100_per_tx | 16 | c | 4 | 250409 | 246803-252012 | 58 us | 76 us | 96 us | 142 us | 5.5 | 1.37 | 37 MB | 0 | 3 |
| 100_per_tx | 256 | native | 4 | 386245 | 375637-392347 | 589 us | 844 us | 1.41 ms | 2.09 ms | 9.5 | 3.68 | 59 MB | 0 | 3 |
| 100_per_tx | 256 | c | 4 | 272421 | 209043-272423 | 928 us | 1.03 ms | 1.34 ms | 2.23 ms | 5.5 | 1.48 | 63 MB | 0 | 3 |

#### `set`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1_key_commit | 1 | native | 4 | 158 | 116-219 | 5.58 ms | 8.36 ms | 15.5 ms | 24.3 ms | 68.2 | 0.01 | 17 MB | 0 | 3 |
| 1_key_commit | 1 | c | 4 | 226 | 151-230 | 3.87 ms | 6.17 ms | 10.4 ms | 30.5 ms | 121.3 | 0.02 | 23 MB | 0 | 3 |
| 1_key_commit | 16 | native | 4 | 2140 | 720-3020 | 5.65 ms | 11.0 ms | 26.4 ms | 44.9 ms | 19.3 | 0.04 | 17 MB | 0 | 3 |
| 1_key_commit | 16 | c | 4 | 3539 | 2640-3875 | 3.89 ms | 5.73 ms | 8.41 ms | 47.4 ms | 14.7 | 0.05 | 25 MB | 0 | 3 |
| 1_key_commit | 256 | native | 4 | 29396 | 19380-39154 | 7.91 ms | 12.1 ms | 20.3 ms | 66.3 ms | 12.8 | 0.38 | 21 MB | 0 | 3 |
| 1_key_commit | 256 | c | 4 | 39831 | 15122-41198 | 5.73 ms | 7.42 ms | 21.6 ms | 32.6 ms | 9.2 | 0.36 | 32 MB | 0 | 3 |

#### `scan`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10k/wantall | 1 | native | 4 | 2826078 | 2788546-2832151 | 3.35 ms | 3.82 ms | 5.30 ms | 13.0 ms | 0.2 | 0.51 | 21 MB | 0 | 3 |
| 10k/wantall | 1 | c | 4 | 6912812 | 6482908-7159057 | 1.37 ms | 1.56 ms | 1.85 ms | 2.16 ms | 0.1 | 0.45 | 25 MB | 0 | 3 |
| 10k/iterator | 1 | native | 4 | 2714704 | 2709609-2821079 | 3.46 ms | 3.75 ms | 4.73 ms | 5.16 ms | 0.2 | 0.51 | 22 MB | 0 | 3 |
| 10k/iterator | 1 | c | 4 | 6014277 | 5948013-6086736 | 1.62 ms | 1.81 ms | 2.23 ms | 7.12 ms | 0.1 | 0.46 | 27 MB | 0 | 3 |
| 100k/wantall | 1 | native | 4 | 4136097 | 3337079-4324462 | 23.9 ms | 25.5 ms | 28.2 ms | 29.3 ms | 0.2 | 0.71 | 48 MB | 0 | 3 |
| 100k/wantall | 1 | c | 4 | 6999122 | 6136013-7524045 | 14.0 ms | 15.3 ms | 16.9 ms | 17.8 ms | 0.1 | 0.47 | 40 MB | 0 | 3 |
| 100k/iterator | 1 | native | 4 | 3636164 | 1812294-4236963 | 24.7 ms | 36.8 ms | 48.8 ms | 79.1 ms | 0.2 | 0.68 | 62 MB | 0 | 3 |
| 100k/iterator | 1 | c | 4 | 6961608 | 6899068-7111538 | 13.9 ms | 15.5 ms | 21.2 ms | 23.7 ms | 0.1 | 0.48 | 40 MB | 0 | 3 |

#### `mixed`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 90r_10w_10ops | 1 | native | 4 | 507 | 423-575 | 1.42 ms | 4.49 ms | 5.73 ms | 8.96 ms | 180.5 | 0.09 | 17 MB | 0 | 3 |
| 90r_10w_10ops | 1 | c | 4 | 1239 | 1216-1269 | 467 us | 1.81 ms | 3.76 ms | 7.00 ms | 173.9 | 0.22 | 23 MB | 0 | 3 |
| 90r_10w_10ops | 16 | native | 4 | 6380 | 3527-6752 | 1.76 ms | 5.33 ms | 12.0 ms | 18.7 ms | 116.0 | 0.74 | 22 MB | 0 | 3 |
| 90r_10w_10ops | 16 | c | 4 | 13881 | 13659-14337 | 649 us | 2.55 ms | 6.28 ms | 10.7 ms | 62.4 | 0.89 | 25 MB | 0 | 3 |
| 90r_10w_10ops | 256 | native | 4 | 32985 | 18963-35182 | 6.84 ms | 11.2 ms | 13.6 ms | 33.7 ms | 101.9 | 3.30 | 50 MB | 0 | 3 |
| 90r_10w_10ops | 256 | c | 4 | 27794 | 26930-28485 | 8.70 ms | 11.0 ms | 14.8 ms | 60.6 ms | 52.7 | 1.47 | 36 MB | 0 | 3 |

#### `many_tx`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3r_2w_commit | 1000 | native | 4 | 40603 | 31582-43188 | 19.5 ms | 37.4 ms | 105.2 ms | 417.2 ms | 48.8 | 1.94 | 72 MB | 0 | 3 |
| 3r_2w_commit | 1000 | c | 4 | 34848 | 32496-37021 | 21.4 ms | 39.2 ms | 191.3 ms | 314.7 ms | 25.3 | 0.88 | 50 MB | 0 | 3 |
| 3r_2w_commit | 5000 | native | 4 | 28241 | 24154-34126 | 139.6 ms | 282.2 ms | 568.0 ms | 749.4 ms | 66.4 | 1.88 | 264 MB | 0 | 3 |
| 3r_2w_commit | 5000 | c | 4 | 32734 | 29772-34078 | 121.3 ms | 227.3 ms | 402.0 ms | 602.4 ms | 34.4 | 1.13 | 138 MB | 0 | 3 |

#### `watch`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| setup | 1 | native | 4 | 54.7 | 52.7-58.9 | 1.16 ms | 2.17 ms | 3.17 ms | 5.63 ms | 276.3 | 0.01 | 17 MB | 0 | 3 |
| setup | 1 | c | 4 | 72.7 | 59.6-78.7 | 147 us | 1.36 ms | 2.98 ms | 3.73 ms | 232.9 | 0.02 | 22 MB | 0 | 3 |
| writer_commit | 1 | native | 4 | 54.7 | 52.7-58.9 | 4.44 ms | 11.8 ms | 19.0 ms | 22.7 ms | 0.0 | 0.00 | 17 MB | 0 | 3 |
| writer_commit | 1 | c | 4 | 72.7 | 59.6-78.7 | 3.48 ms | 6.98 ms | 9.43 ms | 12.9 ms | 0.0 | 0.00 | 22 MB | 0 | 3 |
| write_to_fire | 1 | native | 4 | 54.7 | 52.7-58.9 | 15.6 ms | 17.7 ms | 29.5 ms | 32.3 ms | 0.1 | 0.00 | 17 MB | 0 | 3 |
| write_to_fire | 1 | c | 4 | 72.7 | 59.6-78.7 | 11.2 ms | 14.0 ms | 19.0 ms | 21.7 ms | 0.0 | 0.00 | 22 MB | 0 | 3 |
| setup_5000_c256 | 256 | native | 4 | 81041 | 80865-108624 | 2.15 ms | 2.82 ms | 3.65 ms | 4.01 ms | 35.7 | 2.89 | 81 MB | 0 | 3 |
| setup_5000_c256 | 256 | c | 4 | 82495 | 28016-98654 | 3.02 ms | 3.26 ms | 5.29 ms | 5.30 ms | 14.5 | 1.06 | 71 MB | 0 | 3 |
| fire_5000 | 5000 | native | 4 | 114123 | 112266-118944 | 34.8 ms | 41.6 ms | 42.6 ms | 43.8 ms | 9.3 | 1.04 | 88 MB | 0 | 3 |
| fire_5000 | 5000 | c | 4 | 108088 | 40717-121937 | 37.8 ms | 44.6 ms | 46.1 ms | 46.2 ms | 8.1 | 0.73 | 78 MB | 0 | 3 |

#### Native relative to libfdb_c

Ratios of medians, native / libfdb_c, at the same application thread count. Throughput above 1.00 is native faster; latency and CPU below 1.00 is native better.

| workload | variant | conc | threads | ops/s | p50 | p99 | CPU/op |
|---|---|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 4 | 0.09 | 11.39 | 16.40 | 1.07 |
| get | fresh_tx | 16 | 4 | 0.12 | 8.41 | 6.43 | 1.70 |
| get | fresh_tx | 256 | 4 | 0.82 | 1.28 | 1.22 | 1.68 |
| get_seq | 100_per_tx | 1 | 4 | 0.98 | 0.74 | 0.92 | 0.93 |
| get_seq | 100_per_tx | 16 | 4 | 1.04 | 0.86 | 1.28 | 1.96 |
| get_seq | 100_per_tx | 256 | 4 | 1.42 | 0.63 | 1.05 | 1.74 |
| set | 1_key_commit | 1 | 4 | 0.70 | 1.44 | 1.49 | 0.56 |
| set | 1_key_commit | 16 | 4 | 0.60 | 1.45 | 3.14 | 1.32 |
| set | 1_key_commit | 256 | 4 | 0.74 | 1.38 | 0.94 | 1.39 |
| scan | 10k/wantall | 1 | 4 | 0.41 | 2.45 | 2.87 | 2.77 |
| scan | 10k/iterator | 1 | 4 | 0.45 | 2.13 | 2.12 | 2.37 |
| scan | 100k/wantall | 1 | 4 | 0.59 | 1.70 | 1.67 | 2.40 |
| scan | 100k/iterator | 1 | 4 | 0.52 | 1.78 | 2.31 | 2.77 |
| mixed | 90r_10w_10ops | 1 | 4 | 0.41 | 3.04 | 1.52 | 1.04 |
| mixed | 90r_10w_10ops | 16 | 4 | 0.46 | 2.71 | 1.91 | 1.86 |
| mixed | 90r_10w_10ops | 256 | 4 | 1.19 | 0.79 | 0.92 | 1.93 |
| many_tx | 3r_2w_commit | 1000 | 4 | 1.17 | 0.91 | 0.55 | 1.92 |
| many_tx | 3r_2w_commit | 5000 | 4 | 0.86 | 1.15 | 1.41 | 1.93 |
| watch | setup | 1 | 4 | 0.75 | 7.91 | 1.07 | 1.19 |
| watch | writer_commit | 1 | 4 | 0.75 | 1.28 | 2.02 | 1.25 |
| watch | write_to_fire | 1 | 4 | 0.75 | 1.38 | 1.55 | 1.36 |
| watch | setup_5000_c256 | 256 | 4 | 0.98 | 0.71 | 0.69 | 2.47 |
| watch | fire_5000 | 5000 | 4 | 1.06 | 0.92 | 0.92 | 1.15 |

