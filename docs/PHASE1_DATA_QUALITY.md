# NASA Log Data Quality and Parsing Decisions

## Source

Official Internet Traffic Archive:
https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

Only the original July and August 1995 NASA logs are used.
Decompression is permitted; the raw files are not edited.

## Record accounting

| Measure | Count |
|---|---:|
| July physical records | 1,891,715 |
| August physical records | 1,569,898 |
| Total physical records | 3,461,613 |
| Valid parsed records | 3,457,683 |
| Malformed records | 3,930 |
| Invalid request format | 3,897 |
| Invalid outer log format | 33 |

The July raw file ends with an eight-byte fragment
without a terminating newline. It cannot be parsed
as an HTTP log record.

The official archive describes 3,461,612 requests.
Our record-count accounting includes the additional
incomplete fragment, rather than silently dropping it.

A newline-terminated input line is not necessarily a
valid HTTP request.

## Shared parsing policy

- A missing bytes field or "-" becomes zero.
- Invalid records are counted, never silently discarded.
- Log dates and hours use the recorded local clock time.
- Resource paths exclude query-string parameters.
- HTTP error status codes are 400 through 599 inclusive.
- The same parsing contract applies to both pipelines.

## Validation

MongoDB and Hadoop MapReduce both produced:

Q1: 288 rows
Q2: 20 rows
Q3: 1,369 rows

All aggregated query values matched in the full-data
audit, including stored hourly error rates.

See PHASE1_RESULTS.md for execution evidence and hashes.
