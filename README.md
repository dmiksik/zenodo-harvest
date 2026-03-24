# Zenodo dataset metadata: harvest and parsed extract

This repository contains two related outputs built from Zenodo dataset metadata.

## 1. Raw API harvest

The script [`zenodo_harvest.py`](./zenodo_harvest.py) harvests published Zenodo records of type `dataset` from the Zenodo Records API and stores them as JSON Lines.

Current raw output:

- [`zenodo_harvest4/`](./zenodo_harvest4/)

This part of the repository contains:

- the harvesting script
- harvested raw data in `records.jsonl.gz`

See [`zenodo_harvest4/README.md`](./zenodo_harvest4/README.md) for details about the harvested file, its coverage, and its basic statistics.

## 2. Selected and parsed XML dump extract

The directory [`zenodo_dump_dataset_extract/`](./zenodo_dump_dataset_extract/) contains a structured extract derived from the Zenodo XML metadata dump.

Workflow:

- start from the Zenodo XML dump
- select only XML files corresponding to records of type `Dataset`
- parse the selected XML files into structured tables and packaged outputs

Current structured output:

- [`zenodo_dump_dataset_extract/`](./zenodo_dump_dataset_extract/)

See [`zenodo_dump_dataset_extract/README.md`](./zenodo_dump_dataset_extract/README.md) for provenance, selection method, parsing details, package contents, and output files.
