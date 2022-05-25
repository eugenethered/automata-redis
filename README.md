# Automata Redis 
both for conventional key-value & timeseries data.

## Packaging
`python3 -m build`

## Commands
1. Timeseries Range (open ended) `TS.RANGE [KEY] 0 +`
2. Timeseries Range (latest value summarized) `TS.GET [KEY]`
