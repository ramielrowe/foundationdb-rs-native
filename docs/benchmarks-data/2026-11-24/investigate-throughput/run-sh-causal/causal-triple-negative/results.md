#### `causal`

| variant | background conc | client | threads | secs | same-task checks | cross-task checks | violations | backwards rv | errors | background gets/s |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| negative_control | 256 | native | 4 | 22 | 11908 | 455003 | 945 | 0 | 0 | 334928 |
| negative_control | 256 | c | 4 | 22 | 19269 | 240882 | 2885 | 0 | 0 | 179661 |

