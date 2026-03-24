## Data in `zenodo_harvest4`

This directory contains the current raw harvest output produced by:

```bash
python zenodo_harvest.py \
  --out-dir zenodo_harvest4 \
  --date-from 2016-01-01 \
  --window-field created \
  --dedupe-in-memory
```

## What is in `records.jsonl.gz`

`records.jsonl.gz` is a gzip-compressed JSON Lines file harvested from the Zenodo Records API.

- one line = one published Zenodo record as JSON
- filter: `resource_type.type=dataset`
- lower date bound: `created >= 2016-01-01`
- deduplication during harvest: `--dedupe-in-memory`

This is the raw API-level harvest output. Records are stored as returned by Zenodo and are not yet flattened into analytical tables.

For parsed / structured output derived from Zenodo OAI-PMH / DataCite XML, see [`../zenodo_dump_dataset_extract/`](../zenodo_dump_dataset_extract/).
