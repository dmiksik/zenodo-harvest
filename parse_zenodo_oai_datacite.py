
#!/usr/bin/env python3
"""
Parse Zenodo OAI-PMH / DataCite XML records into analytical tables.

Default target format is Parquet (requires pyarrow). JSONL is available as a fallback:
  python parse_zenodo_oai_datacite.py \
      --input-list dataset-files.txt \
      --output-dir zenodo_dataset_extract \
      --output-format parquet

Notes:
- The parser is intentionally conservative. Unknown / unhandled XML elements and attributes
  are logged to issues.jsonl and aggregated into summary.json; they are not printed one-by-one.
- Fatal parse failures do not stop the whole run. A minimal row with parse_ok = False is emitted
  into records.
- xml_sha256 has been removed to avoid re-reading every XML file and slowing the run down.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from lxml import etree

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None


OAI_NS = "http://schema.datacite.org/oai/oai-1.1/"
DC_NS = "http://datacite.org/schema/kernel-4"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
NS = {"oai": OAI_NS, "d": DC_NS, "xml": XML_NS, "xsi": XSI_NS}
XML_LANG = f"{{{XML_NS}}}lang"

HANDLED_RESOURCE_CHILDREN = {
    "alternateIdentifiers",
    "creators",
    "titles",
    "publisher",
    "publicationYear",
    "contributors",
    "subjects",
    "dates",
    "resourceType",
    "identifier",
    "relatedIdentifiers",
    "rightsList",
    "descriptions",
}
HANDLED_ROOT_CHILDREN = {"schemaVersion", "datacentreSymbol", "payload"}

HANDLED_CREATOR_CHILDREN = {"creatorName", "givenName", "familyName", "nameIdentifier", "affiliation"}
HANDLED_CONTRIBUTOR_CHILDREN = {"contributorName", "givenName", "familyName", "nameIdentifier", "affiliation"}

ALLOWED_ATTRS: Dict[str, set[str]] = {
    "resourceType": {"resourceTypeGeneral"},
    "identifier": {"identifierType"},
    "alternateIdentifier": {"alternateIdentifierType"},
    "creatorName": {"nameType", XML_LANG},
    "contributorName": {"nameType", XML_LANG},
    "nameIdentifier": {"nameIdentifierScheme", "nameIdentifierSchemeURI", "schemeURI"},
    "affiliation": {"affiliationIdentifier", "affiliationIdentifierScheme"},
    "contributor": {"contributorType"},
    "subject": {"subjectScheme", "schemeURI", "valueURI", "classificationCode", XML_LANG},
    "date": {"dateType", "dateInformation"},
    "relatedIdentifier": {
        "relatedIdentifierType",
        "relationType",
        "relatedMetadataScheme",
        "schemeURI",
        "schemeType",
        "resourceTypeGeneral",
    },
    "rights": {"rightsURI", "rightsIdentifier", "rightsIdentifierScheme", "schemeURI", XML_LANG},
    "description": {"descriptionType", XML_LANG},
    "title": {"titleType", XML_LANG},
}

PARQUET_SCHEMA = {
    "records": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("source_file_name", "string"),
        ("schema_version", "string"),
        ("datacentre_symbol", "string"),
        ("identifier", "string"),
        ("identifier_type", "string"),
        ("oai_identifier", "string"),
        ("record_url", "string"),
        ("publisher", "string"),
        ("publication_year", "int64"),
        ("resource_type", "string"),
        ("resource_type_general", "string"),
        ("issued_date_raw", "string"),
        ("issued_date", "date32"),
        ("updated_date_raw", "string"),
        ("updated_date", "date32"),
        ("primary_title", "string"),
        ("title_count", "int64"),
        ("description_count", "int64"),
        ("creator_count", "int64"),
        ("contributor_count", "int64"),
        ("subject_count", "int64"),
        ("related_identifier_count", "int64"),
        ("rights_count", "int64"),
        ("alternate_identifier_count", "int64"),
        ("access_rights_uri", "string"),
        ("access_rights_label", "string"),
        ("parse_ok", "bool"),
        ("parse_error", "string"),
    ],
    "titles": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("title_order", "int64"),
        ("title_text", "string"),
        ("title_type", "string"),
        ("xml_lang", "string"),
    ],
    "creators": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("creator_order", "int64"),
        ("affiliation_order", "int64"),
        ("creator_name", "string"),
        ("creator_name_type", "string"),
        ("given_name", "string"),
        ("family_name", "string"),
        ("name_identifier", "string"),
        ("name_identifier_scheme", "string"),
        ("name_identifier_scheme_uri", "string"),
        ("scheme_uri", "string"),
        ("affiliation_name", "string"),
        ("affiliation_identifier", "string"),
        ("affiliation_identifier_scheme", "string"),
    ],
    "contributors": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("contributor_order", "int64"),
        ("affiliation_order", "int64"),
        ("contributor_type", "string"),
        ("contributor_name", "string"),
        ("contributor_name_type", "string"),
        ("given_name", "string"),
        ("family_name", "string"),
        ("name_identifier", "string"),
        ("name_identifier_scheme", "string"),
        ("name_identifier_scheme_uri", "string"),
        ("scheme_uri", "string"),
        ("affiliation_name", "string"),
        ("affiliation_identifier", "string"),
        ("affiliation_identifier_scheme", "string"),
    ],
    "subjects": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("subject_order", "int64"),
        ("subject_text", "string"),
        ("subject_scheme", "string"),
        ("scheme_uri", "string"),
        ("value_uri", "string"),
        ("classification_code", "string"),
        ("xml_lang", "string"),
    ],
    "dates": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("date_order", "int64"),
        ("date_type", "string"),
        ("date_value_raw", "string"),
        ("date_value", "date32"),
        ("date_information", "string"),
    ],
    "related_identifiers": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("related_identifier_order", "int64"),
        ("related_identifier", "string"),
        ("related_identifier_type", "string"),
        ("relation_type", "string"),
        ("related_metadata_scheme", "string"),
        ("scheme_uri", "string"),
        ("scheme_type", "string"),
        ("resource_type_general", "string"),
    ],
    "rights": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("rights_order", "int64"),
        ("rights_text", "string"),
        ("rights_uri", "string"),
        ("rights_identifier", "string"),
        ("rights_identifier_scheme", "string"),
        ("scheme_uri", "string"),
        ("xml_lang", "string"),
    ],
    "descriptions": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("description_order", "int64"),
        ("description_type", "string"),
        ("description_text", "string"),
        ("xml_lang", "string"),
    ],
    "alternate_identifiers": [
        ("record_id", "string"),
        ("source_xml_path", "string"),
        ("alternate_identifier_order", "int64"),
        ("alternate_identifier", "string"),
        ("alternate_identifier_type", "string"),
    ],
}

TABLES = list(PARQUET_SCHEMA.keys())


def eprint(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


def qname_local_name(item: Any) -> str:
    return etree.QName(item).localname if isinstance(item, str) and item.startswith("{") or hasattr(item, "tag") else str(item)


def attr_name(attr_key: str) -> str:
    if attr_key.startswith("{"):
        qn = etree.QName(attr_key)
        if qn.namespace == XML_NS:
            return f"xml:{qn.localname}"
        return f"{{{qn.namespace}}}{qn.localname}"
    return attr_key


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def element_text(el: Optional[etree._Element]) -> Optional[str]:
    if el is None:
        return None
    return normalize_text("".join(el.itertext()))


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    value = value.strip()
    # First accept full ISO date only.
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None




def issue_dict(path: Path, severity: str, code: str, message: str, record_id: Optional[str] = None, context: Optional[str] = None) -> dict:
    return {
        "severity": severity,
        "code": code,
        "message": message,
        "source_xml_path": str(path),
        "source_file_name": path.name,
        "record_id": record_id,
        "context": context,
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


class IssueTracker:
    def __init__(self, issue_tracker) -> None:
        self.issue_tracker = issue_tracker
        self.counts_by_severity: dict[str, int] = defaultdict(int)
        self.counts_by_code: dict[str, int] = defaultdict(int)
        self.counts_by_signature: dict[str, int] = defaultdict(int)

    def report(self, path: Path, severity: str, code: str, message: str, record_id: Optional[str] = None, context: Optional[str] = None) -> None:
        payload = issue_dict(path, severity, code, message, record_id=record_id, context=context)
        self.issue_tracker.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.counts_by_severity[severity] += 1
        self.counts_by_code[code] += 1
        self.counts_by_signature[f"{code}: {message}"] += 1

    def build_summary(self, top_n: int = 50) -> dict[str, Any]:
        top_signatures = sorted(self.counts_by_signature.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
        return {
            "issue_counts_by_severity": dict(sorted(self.counts_by_severity.items())),
            "issue_counts_by_code": dict(sorted(self.counts_by_code.items())),
            "top_issue_signatures": [
                {"signature": sig, "count": count}
                for sig, count in top_signatures
            ],
        }


def check_unknown_attributes(el: etree._Element, path: Path, issue_tracker: IssueTracker, record_id: Optional[str], context: str, strict_unknown: bool) -> None:
    local = etree.QName(el).localname
    allowed = ALLOWED_ATTRS.get(local, set())
    for k in el.attrib:
        if k not in allowed:
            message = f"Unhandled attribute {attr_name(k)!r} on element {local!r}"
            issue_tracker.report(path, "WARNING", "UNHANDLED_ATTRIBUTE", message, record_id=record_id, context=context)
            if strict_unknown:
                raise ValueError(message)


def extract_record_id(record_url: Optional[str], oai_identifier: Optional[str], source_file_name: str) -> Optional[str]:
    if record_url:
        m = re.search(r"/records/(\d+)$", record_url)
        if m:
            return m.group(1)
    if oai_identifier:
        m = re.search(r":(\d+)$", oai_identifier)
        if m:
            return m.group(1)
    m = re.match(r"(\d+)", source_file_name)
    if m:
        return m.group(1)
    return None


def make_empty_rows() -> dict[str, list[dict]]:
    return {table: [] for table in TABLES}




def build_error_record(path: Path, error_message: str) -> dict[str, Any]:
    return {
        "record_id": extract_record_id(None, None, path.name),
        "source_xml_path": str(path),
        "source_file_name": path.name,
        "schema_version": None,
        "datacentre_symbol": None,
        "identifier": None,
        "identifier_type": None,
        "oai_identifier": None,
        "record_url": None,
        "publisher": None,
        "publication_year": None,
        "resource_type": None,
        "resource_type_general": None,
        "issued_date_raw": None,
        "issued_date": None,
        "updated_date_raw": None,
        "updated_date": None,
        "primary_title": None,
        "title_count": 0,
        "description_count": 0,
        "creator_count": 0,
        "contributor_count": 0,
        "subject_count": 0,
        "related_identifier_count": 0,
        "rights_count": 0,
        "alternate_identifier_count": 0,
        "access_rights_uri": None,
        "access_rights_label": None,
        "parse_ok": False,
        "parse_error": error_message,
    }

def parse_record_file(path: Path, issue_tracker: IssueTracker, strict_unknown: bool = False) -> dict[str, list[dict]]:
    rows = make_empty_rows()
    try:
        if not path.exists():
            raise FileNotFoundError(f"No such file or directory: {path}")

        tree = etree.parse(str(path))
        root = tree.getroot()

        # Root-level checks
        for child in root:
            local = etree.QName(child).localname
            if local not in HANDLED_ROOT_CHILDREN:
                message = f"Unhandled root child element {local!r}"
                issue_tracker.report(path, "WARNING", "UNHANDLED_ROOT_CHILD", message, context="/oai_datacite")
                if strict_unknown:
                    raise ValueError(message)

        schema_version = element_text(root.find("oai:schemaVersion", NS))
        datacentre_symbol = element_text(root.find("oai:datacentreSymbol", NS))
        resource = root.find("oai:payload/d:resource", NS)
        if resource is None:
            raise ValueError("Missing /oai_datacite/payload/resource element")

        # Unknown direct children of <resource>
        for child in resource:
            local = etree.QName(child).localname
            if local not in HANDLED_RESOURCE_CHILDREN:
                message = f"Unhandled resource child element {local!r}"
                issue_tracker.report(path, "WARNING", "UNHANDLED_RESOURCE_CHILD", message, context="/oai_datacite/payload/resource")
                if strict_unknown:
                    raise ValueError(message)

        identifier_el = resource.find("d:identifier", NS)
        check_unknown_attributes(identifier_el, path, issue_tracker, None, "/resource/identifier", strict_unknown) if identifier_el is not None else None
        identifier = element_text(identifier_el)
        identifier_type = identifier_el.get("identifierType") if identifier_el is not None else None

        alt_rows = []
        record_url = None
        oai_identifier = None
        for idx, alt in enumerate(resource.findall("d:alternateIdentifiers/d:alternateIdentifier", NS), start=1):
            check_unknown_attributes(alt, path, issue_tracker, None, f"/resource/alternateIdentifiers/alternateIdentifier[{idx}]", strict_unknown)
            alt_value = element_text(alt)
            alt_type = alt.get("alternateIdentifierType")
            alt_rows.append({
                "source_xml_path": str(path),
                "alternate_identifier_order": idx,
                "alternate_identifier": alt_value,
                "alternate_identifier_type": alt_type,
            })
            if alt_type == "URL" and record_url is None:
                record_url = alt_value
            if alt_type == "oai" and oai_identifier is None:
                oai_identifier = alt_value

        record_id = extract_record_id(record_url, oai_identifier, path.name)

        titles = []
        for idx, title_el in enumerate(resource.findall("d:titles/d:title", NS), start=1):
            check_unknown_attributes(title_el, path, issue_tracker, record_id, f"/resource/titles/title[{idx}]", strict_unknown)
            titles.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "title_order": idx,
                "title_text": element_text(title_el),
                "title_type": title_el.get("titleType"),
                "xml_lang": title_el.get(XML_LANG),
            })

        creators = []
        creator_nodes = resource.findall("d:creators/d:creator", NS)
        for creator_idx, creator in enumerate(creator_nodes, start=1):
            for child in creator:
                local = etree.QName(child).localname
                if local not in HANDLED_CREATOR_CHILDREN:
                    message = f"Unhandled creator child element {local!r}"
                    issue_tracker.report(path, "WARNING", "UNHANDLED_CREATOR_CHILD", message, record_id=record_id, context=f"/resource/creators/creator[{creator_idx}]")
                    if strict_unknown:
                        raise ValueError(message)

            creator_name_el = creator.find("d:creatorName", NS)
            if creator_name_el is not None:
                check_unknown_attributes(creator_name_el, path, issue_tracker, record_id, f"/resource/creators/creator[{creator_idx}]/creatorName", strict_unknown)
            given_name = element_text(creator.find("d:givenName", NS))
            family_name = element_text(creator.find("d:familyName", NS))
            creator_name = element_text(creator_name_el)
            creator_name_type = creator_name_el.get("nameType") if creator_name_el is not None else None

            name_ids = creator.findall("d:nameIdentifier", NS)
            if len(name_ids) > 1:
                issue_tracker.report(
                    path,
                    "WARNING",
                    "MULTIPLE_NAME_IDENTIFIERS",
                    f"Creator has {len(name_ids)} nameIdentifier elements; only the first is mapped into creators table",
                    record_id=record_id,
                    context=f"/resource/creators/creator[{creator_idx}]",
                )
                if strict_unknown:
                    raise ValueError("Multiple creator nameIdentifier elements")
            name_id = name_ids[0] if name_ids else None
            if name_id is not None:
                check_unknown_attributes(name_id, path, issue_tracker, record_id, f"/resource/creators/creator[{creator_idx}]/nameIdentifier[1]", strict_unknown)

            affiliations = creator.findall("d:affiliation", NS)
            if not affiliations:
                affiliations = [None]

            for aff_idx, aff in enumerate(affiliations, start=1):
                if aff is not None:
                    check_unknown_attributes(aff, path, issue_tracker, record_id, f"/resource/creators/creator[{creator_idx}]/affiliation[{aff_idx}]", strict_unknown)
                creators.append({
                    "record_id": record_id,
                    "source_xml_path": str(path),
                    "creator_order": creator_idx,
                    "affiliation_order": aff_idx,
                    "creator_name": creator_name,
                    "creator_name_type": creator_name_type,
                    "given_name": given_name,
                    "family_name": family_name,
                    "name_identifier": element_text(name_id),
                    "name_identifier_scheme": name_id.get("nameIdentifierScheme") if name_id is not None else None,
                    "name_identifier_scheme_uri": name_id.get("nameIdentifierSchemeURI") if name_id is not None else None,
                    "scheme_uri": name_id.get("schemeURI") if name_id is not None else None,
                    "affiliation_name": element_text(aff),
                    "affiliation_identifier": aff.get("affiliationIdentifier") if aff is not None else None,
                    "affiliation_identifier_scheme": aff.get("affiliationIdentifierScheme") if aff is not None else None,
                })

        contributors = []
        contributor_nodes = resource.findall("d:contributors/d:contributor", NS)
        for c_idx, contributor in enumerate(contributor_nodes, start=1):
            check_unknown_attributes(contributor, path, issue_tracker, record_id, f"/resource/contributors/contributor[{c_idx}]", strict_unknown)
            for child in contributor:
                local = etree.QName(child).localname
                if local not in HANDLED_CONTRIBUTOR_CHILDREN:
                    message = f"Unhandled contributor child element {local!r}"
                    issue_tracker.report(path, "WARNING", "UNHANDLED_CONTRIBUTOR_CHILD", message, record_id=record_id, context=f"/resource/contributors/contributor[{c_idx}]")
                    if strict_unknown:
                        raise ValueError(message)

            name_el = contributor.find("d:contributorName", NS)
            if name_el is not None:
                check_unknown_attributes(name_el, path, issue_tracker, record_id, f"/resource/contributors/contributor[{c_idx}]/contributorName", strict_unknown)
            given_name = element_text(contributor.find("d:givenName", NS))
            family_name = element_text(contributor.find("d:familyName", NS))
            contributor_name = element_text(name_el)
            contributor_name_type = name_el.get("nameType") if name_el is not None else None

            name_ids = contributor.findall("d:nameIdentifier", NS)
            if len(name_ids) > 1:
                issue_tracker.report(
                    path,
                    "WARNING",
                    "MULTIPLE_NAME_IDENTIFIERS",
                    f"Contributor has {len(name_ids)} nameIdentifier elements; only the first is mapped into contributors table",
                    record_id=record_id,
                    context=f"/resource/contributors/contributor[{c_idx}]",
                )
                if strict_unknown:
                    raise ValueError("Multiple contributor nameIdentifier elements")
            name_id = name_ids[0] if name_ids else None
            if name_id is not None:
                check_unknown_attributes(name_id, path, issue_tracker, record_id, f"/resource/contributors/contributor[{c_idx}]/nameIdentifier[1]", strict_unknown)

            affiliations = contributor.findall("d:affiliation", NS)
            if not affiliations:
                affiliations = [None]

            for aff_idx, aff in enumerate(affiliations, start=1):
                if aff is not None:
                    check_unknown_attributes(aff, path, issue_tracker, record_id, f"/resource/contributors/contributor[{c_idx}]/affiliation[{aff_idx}]", strict_unknown)
                contributors.append({
                    "record_id": record_id,
                    "source_xml_path": str(path),
                    "contributor_order": c_idx,
                    "affiliation_order": aff_idx,
                    "contributor_type": contributor.get("contributorType"),
                    "contributor_name": contributor_name,
                    "contributor_name_type": contributor_name_type,
                    "given_name": given_name,
                    "family_name": family_name,
                    "name_identifier": element_text(name_id),
                    "name_identifier_scheme": name_id.get("nameIdentifierScheme") if name_id is not None else None,
                    "name_identifier_scheme_uri": name_id.get("nameIdentifierSchemeURI") if name_id is not None else None,
                    "scheme_uri": name_id.get("schemeURI") if name_id is not None else None,
                    "affiliation_name": element_text(aff),
                    "affiliation_identifier": aff.get("affiliationIdentifier") if aff is not None else None,
                    "affiliation_identifier_scheme": aff.get("affiliationIdentifierScheme") if aff is not None else None,
                })

        subjects = []
        for idx, subject in enumerate(resource.findall("d:subjects/d:subject", NS), start=1):
            check_unknown_attributes(subject, path, issue_tracker, record_id, f"/resource/subjects/subject[{idx}]", strict_unknown)
            subjects.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "subject_order": idx,
                "subject_text": element_text(subject),
                "subject_scheme": subject.get("subjectScheme"),
                "scheme_uri": subject.get("schemeURI"),
                "value_uri": subject.get("valueURI"),
                "classification_code": subject.get("classificationCode"),
                "xml_lang": subject.get(XML_LANG),
            })

        dates = []
        issued_date_raw = None
        updated_date_raw = None
        issued_date = None
        updated_date = None
        for idx, date_el in enumerate(resource.findall("d:dates/d:date", NS), start=1):
            check_unknown_attributes(date_el, path, issue_tracker, record_id, f"/resource/dates/date[{idx}]", strict_unknown)
            date_type = date_el.get("dateType")
            date_raw = element_text(date_el)
            date_value = parse_date(date_raw)
            dates.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "date_order": idx,
                "date_type": date_type,
                "date_value_raw": date_raw,
                "date_value": date_value,
                "date_information": date_el.get("dateInformation"),
            })
            if date_type == "Issued" and issued_date_raw is None:
                issued_date_raw = date_raw
                issued_date = date_value
            if date_type == "Updated" and updated_date_raw is None:
                updated_date_raw = date_raw
                updated_date = date_value

        resource_type_el = resource.find("d:resourceType", NS)
        check_unknown_attributes(resource_type_el, path, issue_tracker, record_id, "/resource/resourceType", strict_unknown) if resource_type_el is not None else None
        resource_type = element_text(resource_type_el)
        resource_type_general = resource_type_el.get("resourceTypeGeneral") if resource_type_el is not None else None

        related_identifiers = []
        for idx, rel in enumerate(resource.findall("d:relatedIdentifiers/d:relatedIdentifier", NS), start=1):
            check_unknown_attributes(rel, path, issue_tracker, record_id, f"/resource/relatedIdentifiers/relatedIdentifier[{idx}]", strict_unknown)
            related_identifiers.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "related_identifier_order": idx,
                "related_identifier": element_text(rel),
                "related_identifier_type": rel.get("relatedIdentifierType"),
                "relation_type": rel.get("relationType"),
                "related_metadata_scheme": rel.get("relatedMetadataScheme"),
                "scheme_uri": rel.get("schemeURI"),
                "scheme_type": rel.get("schemeType"),
                "resource_type_general": rel.get("resourceTypeGeneral"),
            })

        rights_rows = []
        access_rights_uri = None
        access_rights_label = None
        for idx, rights in enumerate(resource.findall("d:rightsList/d:rights", NS), start=1):
            check_unknown_attributes(rights, path, issue_tracker, record_id, f"/resource/rightsList/rights[{idx}]", strict_unknown)
            rights_uri = rights.get("rightsURI")
            if access_rights_uri is None and rights_uri and rights_uri.startswith("info:eu-repo/semantics/"):
                access_rights_uri = rights_uri
                access_rights_label = rights_uri.rsplit("/", 1)[-1] if "/" in rights_uri else rights_uri
            rights_rows.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "rights_order": idx,
                "rights_text": element_text(rights),
                "rights_uri": rights_uri,
                "rights_identifier": rights.get("rightsIdentifier"),
                "rights_identifier_scheme": rights.get("rightsIdentifierScheme"),
                "scheme_uri": rights.get("schemeURI"),
                "xml_lang": rights.get(XML_LANG),
            })

        descriptions = []
        for idx, desc in enumerate(resource.findall("d:descriptions/d:description", NS), start=1):
            check_unknown_attributes(desc, path, issue_tracker, record_id, f"/resource/descriptions/description[{idx}]", strict_unknown)
            descriptions.append({
                "record_id": record_id,
                "source_xml_path": str(path),
                "description_order": idx,
                "description_type": desc.get("descriptionType"),
                "description_text": element_text(desc),
                "xml_lang": desc.get(XML_LANG),
            })

        primary_title = titles[0]["title_text"] if titles else None
        record_row = {
            "record_id": record_id,
            "source_xml_path": str(path),
            "source_file_name": path.name,
            "schema_version": schema_version,
            "datacentre_symbol": datacentre_symbol,
            "identifier": identifier,
            "identifier_type": identifier_type,
            "oai_identifier": oai_identifier,
            "record_url": record_url,
            "publisher": element_text(resource.find("d:publisher", NS)),
            "publication_year": safe_int(element_text(resource.find("d:publicationYear", NS))),
            "resource_type": resource_type,
            "resource_type_general": resource_type_general,
            "issued_date_raw": issued_date_raw,
            "issued_date": issued_date,
            "updated_date_raw": updated_date_raw,
            "updated_date": updated_date,
            "primary_title": primary_title,
            "title_count": len(titles),
            "description_count": len(descriptions),
            "creator_count": len(creator_nodes),
            "contributor_count": len(contributor_nodes),
            "subject_count": len(subjects),
            "related_identifier_count": len(related_identifiers),
            "rights_count": len(rights_rows),
            "alternate_identifier_count": len(alt_rows),
            "access_rights_uri": access_rights_uri,
            "access_rights_label": access_rights_label,
                "parse_ok": True,
            "parse_error": None,
        }

        # Backfill record_id/source_xml_path in alternate id rows now that record_id is known
        for row in alt_rows:
            row["record_id"] = record_id

        rows["records"].append(record_row)
        rows["titles"].extend(titles)
        rows["creators"].extend(creators)
        rows["contributors"].extend(contributors)
        rows["subjects"].extend(subjects)
        rows["dates"].extend(dates)
        rows["related_identifiers"].extend(related_identifiers)
        rows["rights"].extend(rights_rows)
        rows["descriptions"].extend(descriptions)
        rows["alternate_identifiers"].extend(alt_rows)
        return rows

    except Exception as exc:
        issue_code = "MISSING_INPUT_FILE" if isinstance(exc, FileNotFoundError) else "PARSE_EXCEPTION"
        issue_tracker.report(path, "ERROR", issue_code, str(exc), record_id=None, context="parse_record_file")
        rows["records"].append(build_error_record(path, str(exc)))
        return rows


def safe_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def build_pyarrow_schema(table_name: str):
    if pa is None:
        raise RuntimeError("pyarrow is not available")
    type_map = {
        "string": pa.string(),
        "int64": pa.int64(),
        "bool": pa.bool_(),
        "date32": pa.date32(),
    }
    return pa.schema([pa.field(name, type_map[dtype]) for name, dtype in PARQUET_SCHEMA[table_name]])


class TableWriter:
    def __init__(self, output_dir: Path, output_format: str) -> None:
        self.output_dir = output_dir
        self.output_format = output_format
        self.part_counter = defaultdict(int)
        if output_format == "parquet" and pa is None:
            raise SystemExit("Output format 'parquet' requires pyarrow. Install it first, e.g. `pip install pyarrow`.")
        for table in TABLES:
            (self.output_dir / table).mkdir(parents=True, exist_ok=True)

    def flush_table(self, table_name: str, rows: List[dict]) -> None:
        if not rows:
            return
        self.part_counter[table_name] += 1
        part_no = self.part_counter[table_name]
        table_dir = self.output_dir / table_name

        if self.output_format == "jsonl":
            out_path = table_dir / f"part-{part_no:05d}.jsonl"
            with out_path.open("w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")
            return

        schema = build_pyarrow_schema(table_name)
        batch = {}
        for field in schema:
            name = field.name
            batch[name] = [row.get(name) for row in rows]
        out_path = table_dir / f"part-{part_no:05d}.parquet"
        pq.write_table(pa.Table.from_pydict(batch, schema=schema), out_path)

    def flush_all(self, buffers: Dict[str, List[dict]]) -> None:
        for table_name, rows in buffers.items():
            self.flush_table(table_name, rows)


def json_default(obj: Any):
    if isinstance(obj, (dt.date, dt.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type not JSON serializable: {type(obj)!r}")


def iter_input_files(input_list: Optional[Path], input_dir: Optional[Path]) -> Iterator[Path]:
    if input_list:
        list_dir = input_list.resolve().parent
        with input_list.open("r", encoding="utf-8") as fh:
            for line in fh:
                value = line.strip()
                if not value:
                    continue
                path = Path(value)
                if not path.is_absolute():
                    path = list_dir / path
                yield path
    elif input_dir:
        for path in sorted(input_dir.rglob("*.xml")):
            yield path
    else:
        raise ValueError("Either --input-list or --input-dir must be provided")


def merge_rows(target: Dict[str, List[dict]], new_rows: Dict[str, List[dict]]) -> None:
    for table in TABLES:
        target[table].extend(new_rows.get(table, []))


def empty_buffers() -> Dict[str, List[dict]]:
    return {table: [] for table in TABLES}


def count_total_inputs(input_list: Optional[Path], input_dir: Optional[Path]) -> Optional[int]:
    if input_list:
        with input_list.open("r", encoding="utf-8") as fh:
            return sum(1 for line in fh if line.strip())
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Zenodo OAI-PMH / DataCite XML into analytical tables.")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-list", type=Path, help="Text file with one XML path per line.")
    src.add_argument("--input-dir", type=Path, help="Root directory with XML files.")
    ap.add_argument("--output-dir", type=Path, required=True, help="Output directory for parquet/jsonl tables.")
    ap.add_argument("--output-format", choices=["parquet", "jsonl"], default="parquet", help="Output format. Parquet requires pyarrow.")
    ap.add_argument("--batch-size", type=int, default=10000, help="Flush rows every N input files.")
    ap.add_argument("--progress-every", type=int, default=1000, help="Print progress every N input files.")
    ap.add_argument("--strict-unknown", action="store_true", help="Treat unhandled XML elements/attributes as fatal for that file.")
    args = ap.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    issues_path = args.output_dir / "issues.jsonl"
    summary_path = args.output_dir / "summary.json"

    total = count_total_inputs(args.input_list, args.input_dir)
    writer = TableWriter(args.output_dir, args.output_format)
    buffers = empty_buffers()

    processed = 0
    fatal_errors = 0
    started = time.time()

    eprint(f"[INFO] Starting. output={args.output_dir} format={args.output_format}")

    with issues_path.open("w", encoding="utf-8") as issues_fp:
        issue_tracker = IssueTracker(issues_fp)
        for xml_path in iter_input_files(args.input_list, args.input_dir):
            processed += 1
            rows = parse_record_file(xml_path, issue_tracker, strict_unknown=args.strict_unknown)
            if rows["records"] and rows["records"][0].get("parse_ok") is False:
                fatal_errors += 1
            merge_rows(buffers, rows)

            if processed % args.batch_size == 0:
                writer.flush_all(buffers)
                buffers = empty_buffers()

            if processed % args.progress_every == 0:
                elapsed = max(time.time() - started, 0.001)
                rate = processed / elapsed
                if total:
                    eprint(f"[INFO] {processed}/{total} ({processed / total:.1%}), rate={rate:.1f} files/s, fatal={fatal_errors}")
                else:
                    eprint(f"[INFO] {processed} files, rate={rate:.1f} files/s, fatal={fatal_errors}")

        writer.flush_all(buffers)

    elapsed = time.time() - started
    summary = {
        "processed_files": processed,
        "fatal_parse_errors": fatal_errors,
        "elapsed_seconds": elapsed,
        "files_per_second": (processed / elapsed) if elapsed > 0 else None,
        "output_dir": str(args.output_dir),
        "output_format": args.output_format,
        "batch_size": args.batch_size,
        "progress_every": args.progress_every,
        "strict_unknown": args.strict_unknown,
        "issues_jsonl": str(issues_path),
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        **issue_tracker.build_summary(),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    eprint(f"[INFO] Done. processed={processed}, fatal_parse_errors={fatal_errors}, elapsed={elapsed:.1f}s, rate={(processed / elapsed) if elapsed > 0 else 0:.1f} files/s, output={args.output_dir}")


if __name__ == "__main__":
    main()
