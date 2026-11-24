#### `causal`

| variant | background conc | client | threads | secs | same-task checks | cross-task checks | violations | backwards rv | errors | background gets/s |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| negative_control | 256 | native | 4 | 22 | 30197 | 538814 | 1276 | 0 | 0 | 392128 |
| negative_control | 256 | c | 4 | 22 | 30319 | 221546 | 9746 | 0 | 0 | 162486 |

