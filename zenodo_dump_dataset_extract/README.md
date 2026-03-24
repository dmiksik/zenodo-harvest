# Zenodo Dataset Metadata Extract

Parsed metadata extract of published Zenodo dataset records, produced from Zenodo OAI-PMH / DataCite XML.

## What this package contains

This package contains a structured analytical extract split into multiple tables.

Main tables:

- `records` — one row per record
- `titles` — titles
- `creators` — creators and their affiliations
- `contributors` — contributors and their affiliations
- `subjects` — keywords / subjects
- `dates` — additional dates
- `related_identifiers` — related identifiers
- `rights` — rights / licence information
- `descriptions` — descriptions / abstracts
- `alternate_identifiers` — alternate identifiers

The parser also produces:

- `summary.json` — run summary and aggregated warning counts
- `issues.jsonl` — detailed parser warnings and errors

The default parser output format is Parquet.

## Data packages

Two package variants are provided.

### 1. Parquet package

Archive with the original parsed tables and logs:

- `records/*.parquet`
- `titles/*.parquet`
- `creators/*.parquet`
- `contributors/*.parquet`
- `subjects/*.parquet`
- `dates/*.parquet`
- `related_identifiers/*.parquet`
- `rights/*.parquet`
- `descriptions/*.parquet`
- `alternate_identifiers/*.parquet`
- `summary.json`
- `issues.jsonl`

This is the most complete raw analytical form of the extract.

### 2. DuckDB package

Single-file database version of the same extract:

- `zenodo_extract.duckdb`
- `summary.json`
- `issues.jsonl`

The DuckDB file contains the same tables as the Parquet package, but packed into one database file for easier querying and sharing.

## Record coverage

This extract was produced from **733,319** XML input files.

Run summary:

- `processed_files`: 733319
- `fatal_parse_errors`: 0

The parser version used for this release includes `records.version`.

## Important notes

This extract is intentionally conservative.

Unknown or currently unhandled XML elements and attributes are logged to `issues.jsonl` and aggregated in `summary.json`; they do not stop the run. Fatal parse failures would appear in `records` as rows with `parse_ok = false`, but this run completed with zero fatal parse errors.

Current known unhandled resource-level elements include:

- `language`
- `fundingReferences`
- `geoLocations`

These are not yet mapped into output tables.

For creators and contributors with multiple `nameIdentifier` elements, only the first identifier is mapped into the output tables.

## Main table: `records`

`records` is the primary table, with one row per source record.

It includes core fields such as:

- record identifiers
- source XML file path
- publisher
- publication year
- resource type
- primary title
- record URL
- counts of related values in child tables
- parse status
- `version`

## Example usage with DuckDB

Open the database:

```bash
duckdb zenodo_extract.duckdb
```

Example query:

```sql
SELECT record_id, identifier, primary_title, publication_year, version
FROM records
WHERE version IS NOT NULL
LIMIT 20;
```

Join records with creators:

```sql
SELECT r.record_id, r.primary_title, c.creator_order, c.creator_name
FROM records r
JOIN creators c USING (record_id)
LIMIT 20;
```

## Example usage with Parquet directly

DuckDB can also query the Parquet files directly:

```sql
SELECT record_id, primary_title, version
FROM read_parquet('records/*.parquet')
WHERE version IS NOT NULL
LIMIT 20;
```

## Provenance

Source: Zenodo OAI-PMH / DataCite XML dataset records.

This package is a parsed analytical extract, not the original raw XML dump.

