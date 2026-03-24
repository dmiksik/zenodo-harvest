
# Zenodo OAI-PMH / DataCite XML → analytical tables (v2)

Tento dokument popisuje přesný mapping z XML do tabulek, které generuje `parse_zenodo_oai_datacite.py`.

Ve verzi v2 je navíc mapován element `version` do sloupce `records.version`.

## Obecná pravidla

- Parsuje se XML obálka `oai_datacite` (namespace `http://schema.datacite.org/oai/oai-1.1/`).
- Hlavní payload je `/oai_datacite/payload/resource` (namespace `http://datacite.org/schema/kernel-4`).
- Textová hodnota elementu se ukládá jako **normalizovaný text**:
  - sloučí se všechny textové uzly přes `itertext()`
  - více mezer / nových řádků se znormalizuje na jednu mezeru
  - výsledek se ořízne (`strip`)
- Chybějící element/atribut => `NULL`
- Datum se ukládá dvojím způsobem:
  - `*_raw`: původní text z XML
  - `*_date`: převod jen pokud jde o čisté ISO datum `YYYY-MM-DD`, jinak `NULL`
- Neočekávané / nezpracované elementy a atributy se zapisují do `issues.jsonl` a vypisují na `stderr`
- Při fatální chybě se zpracování ostatních souborů nezastaví; do `records` se uloží minimální řádek s `parse_ok = false`

## Identifikace záznamu

`record_id` se odvozuje v tomto pořadí:

1. z `alternateIdentifier` typu `URL`, regex `/records/<id>`
2. z `alternateIdentifier` typu `oai`, regex `:<id>`
3. ze jména vstupního souboru, pokud začíná číslem

---

## 1) `records`

Jeden řádek = jeden XML soubor / jeden record.

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené dle pravidel výše |
| `source_xml_path` | cesta ke vstupnímu XML souboru |
| `source_file_name` | basename vstupního XML souboru |
| `schema_version` | `/oai_datacite/schemaVersion` |
| `datacentre_symbol` | `/oai_datacite/datacentreSymbol` |
| `identifier` | `/oai_datacite/payload/resource/identifier` text |
| `identifier_type` | `/oai_datacite/payload/resource/identifier/@identifierType` |
| `oai_identifier` | první `alternateIdentifier` s `@alternateIdentifierType="oai"` |
| `record_url` | první `alternateIdentifier` s `@alternateIdentifierType="URL"` |
| `publisher` | `/resource/publisher` |
| `publication_year` | `/resource/publicationYear` jako integer |
| `resource_type` | `/resource/resourceType` text |
| `resource_type_general` | `/resource/resourceType/@resourceTypeGeneral` |
| `version` | `/resource/version` text |
| `issued_date_raw` | první `/resource/dates/date[@dateType="Issued"]` text |
| `issued_date` | `issued_date_raw` převedené na `DATE` pouze pokud je ve formátu `YYYY-MM-DD` |
| `updated_date_raw` | první `/resource/dates/date[@dateType="Updated"]` text |
| `updated_date` | `updated_date_raw` převedené na `DATE` pouze pokud je ve formátu `YYYY-MM-DD` |
| `primary_title` | první `/resource/titles/title` text |
| `title_count` | počet `/resource/titles/title` |
| `description_count` | počet `/resource/descriptions/description` |
| `creator_count` | počet `/resource/creators/creator` |
| `contributor_count` | počet `/resource/contributors/contributor` |
| `subject_count` | počet `/resource/subjects/subject` |
| `related_identifier_count` | počet `/resource/relatedIdentifiers/relatedIdentifier` |
| `rights_count` | počet `/resource/rightsList/rights` |
| `alternate_identifier_count` | počet `/resource/alternateIdentifiers/alternateIdentifier` |
| `access_rights_uri` | první `/resource/rightsList/rights/@rightsURI`, která začíná `info:eu-repo/semantics/` |
| `access_rights_label` | poslední segment z `access_rights_uri` (např. `openAccess`, `restrictedAccess`) |
| `xml_sha256` | SHA-256 hash původního XML souboru |
| `parse_ok` | `true` při úspěšném parse, `false` při fatální chybě |
| `parse_error` | text chyby při fatálním parse, jinak `NULL` |

---

## 2) `titles`

Jeden řádek = jeden `title`.

XPath: `/oai_datacite/payload/resource/titles/title`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `title_order` | pořadí elementu v rámci recordu, od 1 |
| `title_text` | text `title` |
| `title_type` | `@titleType` |
| `xml_lang` | `@xml:lang` |

---

## 3) `creators`

Jeden řádek = **jeden creator × jedna affiliation**.

Pravidlo:
- pokud creator nemá žádnou `affiliation`, vytvoří se **jeden řádek** s `affiliation_* = NULL`
- pokud má creator N afiliací, vytvoří se N řádků se stejným `creator_order` a různým `affiliation_order`

XPath: `/oai_datacite/payload/resource/creators/creator`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `creator_order` | pořadí `creator` v rámci recordu, od 1 |
| `affiliation_order` | pořadí `affiliation` v rámci daného autora, od 1; při absenci afiliace = 1 |
| `creator_name` | `creator/creatorName` text |
| `creator_name_type` | `creator/creatorName/@nameType` |
| `given_name` | `creator/givenName` |
| `family_name` | `creator/familyName` |
| `name_identifier` | první `creator/nameIdentifier` text |
| `name_identifier_scheme` | první `creator/nameIdentifier/@nameIdentifierScheme` |
| `name_identifier_scheme_uri` | první `creator/nameIdentifier/@nameIdentifierSchemeURI` |
| `scheme_uri` | první `creator/nameIdentifier/@schemeURI` |
| `affiliation_name` | text daného `creator/affiliation` |
| `affiliation_identifier` | `creator/affiliation/@affiliationIdentifier` |
| `affiliation_identifier_scheme` | `creator/affiliation/@affiliationIdentifierScheme` |

Poznámka:
- pokud má creator více `nameIdentifier`, parser zaloguje warning `MULTIPLE_NAME_IDENTIFIERS` a do tabulky uloží jen první

---

## 4) `contributors`

Jeden řádek = **jeden contributor × jedna affiliation**.

Pravidla stejná jako u `creators`.

XPath: `/oai_datacite/payload/resource/contributors/contributor`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `contributor_order` | pořadí `contributor` v rámci recordu, od 1 |
| `affiliation_order` | pořadí affiliation v rámci contributoru; při absenci afiliace = 1 |
| `contributor_type` | `contributor/@contributorType` |
| `contributor_name` | `contributor/contributorName` text |
| `contributor_name_type` | `contributor/contributorName/@nameType` |
| `given_name` | `contributor/givenName` |
| `family_name` | `contributor/familyName` |
| `name_identifier` | první `contributor/nameIdentifier` text |
| `name_identifier_scheme` | první `contributor/nameIdentifier/@nameIdentifierScheme` |
| `name_identifier_scheme_uri` | první `contributor/nameIdentifier/@nameIdentifierSchemeURI` |
| `scheme_uri` | první `contributor/nameIdentifier/@schemeURI` |
| `affiliation_name` | text daného `contributor/affiliation` |
| `affiliation_identifier` | `contributor/affiliation/@affiliationIdentifier` |
| `affiliation_identifier_scheme` | `contributor/affiliation/@affiliationIdentifierScheme` |

---

## 5) `subjects`

Jeden řádek = jeden `subject`.

XPath: `/oai_datacite/payload/resource/subjects/subject`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `subject_order` | pořadí `subject`, od 1 |
| `subject_text` | text `subject` |
| `subject_scheme` | `@subjectScheme` |
| `scheme_uri` | `@schemeURI` |
| `value_uri` | `@valueURI` |
| `classification_code` | `@classificationCode` |
| `xml_lang` | `@xml:lang` |

---

## 6) `dates`

Jeden řádek = jeden `date`.

XPath: `/oai_datacite/payload/resource/dates/date`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `date_order` | pořadí `date`, od 1 |
| `date_type` | `@dateType` |
| `date_value_raw` | text elementu `date` |
| `date_value` | převod `date_value_raw` na `DATE` pouze pro čisté ISO datum `YYYY-MM-DD` |
| `date_information` | `@dateInformation` |

---

## 7) `related_identifiers`

Jeden řádek = jeden `relatedIdentifier`.

XPath: `/oai_datacite/payload/resource/relatedIdentifiers/relatedIdentifier`

Žádná interpretace typu concept DOI se v tomto kroku nedělá.

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `related_identifier_order` | pořadí `relatedIdentifier`, od 1 |
| `related_identifier` | text `relatedIdentifier` |
| `related_identifier_type` | `@relatedIdentifierType` |
| `relation_type` | `@relationType` |
| `related_metadata_scheme` | `@relatedMetadataScheme` |
| `scheme_uri` | `@schemeURI` |
| `scheme_type` | `@schemeType` |
| `resource_type_general` | `@resourceTypeGeneral` |

---

## 8) `rights`

Jeden řádek = jeden `rights`.

XPath: `/oai_datacite/payload/resource/rightsList/rights`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `rights_order` | pořadí `rights`, od 1 |
| `rights_text` | text `rights` |
| `rights_uri` | `@rightsURI` |
| `rights_identifier` | `@rightsIdentifier` |
| `rights_identifier_scheme` | `@rightsIdentifierScheme` |
| `scheme_uri` | `@schemeURI` |
| `xml_lang` | `@xml:lang` |

---

## 9) `descriptions`

Jeden řádek = jeden `description`.

XPath: `/oai_datacite/payload/resource/descriptions/description`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `description_order` | pořadí `description`, od 1 |
| `description_type` | `@descriptionType` |
| `description_text` | text `description` |
| `xml_lang` | `@xml:lang` |

---

## 10) `alternate_identifiers`

Jeden řádek = jeden `alternateIdentifier`.

XPath: `/oai_datacite/payload/resource/alternateIdentifiers/alternateIdentifier`

| sloupec | XML mapping |
|---|---|
| `record_id` | odvozené |
| `source_xml_path` | cesta ke zdrojovému XML |
| `alternate_identifier_order` | pořadí `alternateIdentifier`, od 1 |
| `alternate_identifier` | text `alternateIdentifier` |
| `alternate_identifier_type` | `@alternateIdentifierType` |

---

## Co parser považuje za „překvapení“

Parser loguje issues pro:

- neobsloužený přímý child element `oai_datacite`
- neobsloužený přímý child element `resource` (s výjimkou elementu `version`, který je ve v2 mapován do `records.version`)
- neobsloužený child element uvnitř `creator`
- neobsloužený child element uvnitř `contributor`
- neobsloužený atribut na elementech, které parser zná
- více `nameIdentifier` u `creator` / `contributor`
- fatální parse exception (nevalidní XML, chybějící `payload/resource`, atd.)

Issues se zapisují do `issues.jsonl` se základní identifikací souboru:
- `source_xml_path`
- `source_file_name`
- `record_id` (pokud už šlo odvodit)
- `severity`
- `code`
- `message`
- `context`

---

## Poznámka k rozsahu v1

Parser **zatím vědomě neukládá** některé DataCite bloky, které se mohou v dumpu objevit, např.:

- `language`
- `sizes`
- `formats`
- `fundingReferences`
- `geoLocations`

Pokud se objeví, parser je **zachytí jako issue** (`UNHANDLED_RESOURCE_CHILD`), takže půjde přesně dohledat, ve kterých souborech se tyto struktury vyskytly.

Element `version` do této skupiny ve v2 už nepatří: mapuje se do `records.version` jako normalizovaný text z `/resource/version`.
