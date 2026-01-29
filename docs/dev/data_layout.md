# Data layout

- `data/raw/`: immutable raw snapshots (HTML/CSV) by source and date
- `data/normalized/`: canonical diffable NDJSON/CSV used for long-term storage
- `data/db/`: regeneratable DuckDB + parquet caches
