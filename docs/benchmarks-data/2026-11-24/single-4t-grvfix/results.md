#### `get`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fresh_tx | 1 | native | 4 | 13175 | 9370-17945 | 58 us | 95 us | 223 us | 2.00 ms | 29.9 | 0.39 | 17 MB | 0 | 3 |
| fresh_tx | 16 | native | 4 | 160382 | 100655-167481 | 97 us | 117 us | 162 us | 295 us | 15.0 | 2.34 | 24 MB | 0 | 3 |
| fresh_tx | 256 | native | 4 | 329246 | 320547-331684 | 751 us | 946 us | 1.60 ms | 2.06 ms | 10.7 | 3.54 | 47 MB | 0 | 3 |

#### `get_seq`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100_per_tx | 1 | native | 4 | 33866 | 32319-34708 | 28 us | 29 us | 39 us | 62 us | 13.3 | 0.45 | 18 MB | 0 | 3 |
| 100_per_tx | 16 | native | 4 | 277686 | 248733-303781 | 51 us | 68 us | 146 us | 359 us | 10.0 | 2.79 | 31 MB | 0 | 3 |
| 100_per_tx | 256 | native | 4 | 418162 | 312223-427596 | 564 us | 755 us | 1.10 ms | 1.56 ms | 8.9 | 3.71 | 64 MB | 0 | 3 |

#### `set`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1_key_commit | 1 | native | 4 | 224 | 157-307 | 3.43 ms | 6.37 ms | 12.3 ms | 15.6 ms | 63.2 | 0.01 | 17 MB | 0 | 3 |
| 1_key_commit | 16 | native | 4 | 4461 | 1204-4495 | 3.47 ms | 3.79 ms | 6.58 ms | 12.2 ms | 21.6 | 0.09 | 18 MB | 0 | 3 |
| 1_key_commit | 256 | native | 4 | 39616 | 13327-40934 | 5.71 ms | 6.69 ms | 24.4 ms | 32.7 ms | 14.8 | 0.58 | 22 MB | 0 | 3 |

#### `mixed`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 90r_10w_10ops | 1 | native | 4 | 1456 | 951-1480 | 324 us | 3.31 ms | 3.71 ms | 8.59 ms | 151.1 | 0.22 | 17 MB | 0 | 3 |
| 90r_10w_10ops | 16 | native | 4 | 16073 | 7751-16092 | 526 us | 2.06 ms | 6.39 ms | 11.2 ms | 112.8 | 1.79 | 20 MB | 0 | 3 |
| 90r_10w_10ops | 256 | native | 4 | 35718 | 33369-35909 | 6.60 ms | 9.18 ms | 12.8 ms | 52.7 ms | 91.3 | 3.28 | 37 MB | 0 | 3 |

#### `many_tx`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3r_2w_commit | 1000 | native | 4 | 41754 | 41464-43234 | 18.8 ms | 32.3 ms | 145.5 ms | 346.5 ms | 44.1 | 1.86 | 73 MB | 0 | 3 |
| 3r_2w_commit | 5000 | native | 4 | 31424 | 28413-32008 | 132.0 ms | 238.2 ms | 552.8 ms | 725.7 ms | 55.8 | 1.79 | 264 MB | 0 | 3 |

#### `watch`

| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| setup | 1 | native | 4 | 58.0 | 56.8-60.1 | 128 us | 1.14 ms | 1.51 ms | 3.25 ms | 317.1 | 0.02 | 17 MB | 0 | 3 |
| writer_commit | 1 | native | 4 | 58.0 | 56.8-60.1 | 3.44 ms | 13.5 ms | 20.9 ms | 195.8 ms | 0.0 | 0.00 | 17 MB | 0 | 3 |
| write_to_fire | 1 | native | 4 | 58.0 | 56.8-60.1 | 12.5 ms | 23.4 ms | 26.6 ms | 200.4 ms | 0.1 | 0.00 | 17 MB | 0 | 3 |
| setup_5000_c256 | 256 | native | 4 | 108284 | 74855-129781 | 1.58 ms | 3.25 ms | 4.37 ms | 4.88 ms | 33.2 | 3.17 | 81 MB | 0 | 3 |
| fire_5000 | 5000 | native | 4 | 121968 | 119451-129574 | 33.1 ms | 39.7 ms | 40.9 ms | 41.0 ms | 8.2 | 1.05 | 87 MB | 0 | 3 |

