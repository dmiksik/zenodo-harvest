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

## Current file statistics

- total records: **565,713**
- all records are `dataset`
- all records have a DOI
- `created` range: **2016-01-01T03:30:24+00:00** to **2026-03-20T23:54:24.726156+00:00**
- `updated` range: **2020-01-21T07:21:29.575210+00:00** to **2026-03-21T10:08:40.399327+00:00**
- records with files: **535,041** (94.6%)
- records with license: **548,246** (96.9%)

Access rights:
- `open`: **535,043** (94.6%)
- `restricted`: **27,605** (4.9%)
- `embargoed`: **3,065** (0.5%)

## Record structure

Top-level fields include identifiers, timestamps, file information, links, statistics, and a nested `metadata` object.

Typical top-level keys:
`conceptdoi`, `conceptrecid`, `created`, `doi`, `doi_url`, `files`, `id`, `links`, `metadata`, `modified`, `owners`, `recid`, `revision`, `state`, `stats`, `status`, `submitted`, `swh`, `title`, `updated`.

Typical `metadata` keys:
`access_right`, `creators`, `description`, `doi`, `journal`, `license`, `publication_date`, `relations`, `resource_type`, `title`.

For parsed / structured output derived from Zenodo OAI-PMH / DataCite XML, see [`../zenodo_dump_dataset_extract/`](../zenodo_dump_dataset_extract/).
