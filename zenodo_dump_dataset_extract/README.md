# Zenodo Dataset Metadata Extract

Parsed metadata extract of published Zenodo dataset records, produced from Zenodo OAI-PMH / DataCite XML.

## Provenance

### 1. Source dump

The starting point was the latest available Zenodo bulk XML metadata dump at the time of processing:

- dump type: `records-xml.tar.gz`
- dump creation time: `2026-02-07T00:07:24.920272+00:00`
- exact dump URL: `https://zenodo.org/api/exporter/records-xml.tar.gz/72eafab9-4090-448b-9163-39fcd7e28715`

The XML dump archive contains one XML file per record, named `<record_id>.xml`.

### 2. Selection of dataset records

From the unpacked XML dump, we selected only those XML files that contained the exact string:

```text
<resourceType resourceTypeGeneral="Dataset">
```

The initial filename extraction command was:

```bash
PATTERN='<resourceType resourceTypeGeneral="Dataset">'

LC_ALL=C find zenodo-records -type f -name '*.xml' -print0   | xargs -0 -r grep -lZF -- "$PATTERN"   | xargs -0 -n1 basename   > dataset-filenames.txt
```

### 3. File list used for parsing

The list of selected XML filenames is included in this directory as:

- [`dataset-filenames.txt`](./dataset-filenames.txt)

Only XML files listed there were passed to the parser.

### 4. Parsing

The selected XML files were then parsed into the structured extract described below.

## Contents

This package contains a structured extract split into these tables:

- `records`
- `titles`
- `creators`
- `contributors`
- `subjects`
- `dates`
- `related_identifiers`
- `rights`
- `descriptions`
- `alternate_identifiers`

Additional files:

- [`summary.json`](./summary.json) — run summary
- [`issues.jsonl`](./issues.jsonl) — parser warnings and errors

Two package variants are provided:

- **Parquet** — [`zenodo_dataset_extract_parquet_2026-03-24.tar.gz`](./zenodo_dataset_extract_parquet_2026-03-24.tar.gz)
- **DuckDB** — [`zenodo_dataset_extract_duckdb_2026-03-24.tar.gz`](./zenodo_dataset_extract_duckdb_2026-03-24.tar.gz)

## Coverage

This extract was produced from **733,319** XML input files.

- `processed_files`: 733319
- `fatal_parse_errors`: 0

The parser version used for this release includes `records.version`.

## Notes

This extract is intentionally conservative.

Currently unhandled resource-level elements include:

- `language`
- `fundingReferences`
- `geoLocations`

For creators and contributors with multiple `nameIdentifier` elements, only the first identifier is mapped.

## Main table

`records` contains one row per source record and includes core fields such as identifiers, source XML path, publisher, publication year, resource type, primary title, record URL, parse status, and `version`.

## Example

```bash
duckdb zenodo_extract.duckdb
```

```sql
SELECT record_id, identifier, primary_title, publication_year, version
FROM records
WHERE version IS NOT NULL
LIMIT 20;
```
