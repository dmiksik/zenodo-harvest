#!/usr/bin/env bash
set -euo pipefail

DB="${DB:-zenodo_dump/zenodo.duckdb}"
INST="${INST:-seznam_vo.txt}"
COMMC="${COMMC:-cz_communities.csv}"
OUTDIR="${OUTDIR:-zenodo_dump/audit}"
THREADS="${THREADS:-4}"

need_duckdb() {
  command -v duckdb >/dev/null 2>&1 || {
    echo "❌ duckdb CLI není v PATH. Nainstaluj např.: sudo apt-get install duckdb" >&2
    exit 1
  }
}

run_sql() {
  duckdb "$DB" <<SQL
PRAGMA threads=$THREADS;
.timer on;
$(cat)
SQL
}

help() {
  cat <<EOF
rules.sh — správa CZ příznaků v DuckDB

ENV:
  DB=$DB
  INST=$INST
  COMMC=$COMMC
  OUTDIR=$OUTDIR
  THREADS=$THREADS

Příkazy:
  init                     — inicializace (makra, tabulky, prázdný records_cz)
  load-institutions        — načte seznam institucí z INST do cz_institutions (s normalizací)
  load-communities
  rule-inst-v1 [minlen=8]
  rule-affkw-v1 [regex]    — klíčová slova v affiliaci (regex nad affil_norm)
  rule-grants-v1 [regex]   — granty (regex nad funder name)
  rule-location-v1         — CZ v locations (country_code='CZ' nebo regex v country)
  rule-text-v1 [regex]     — text (title+description) regex
  rule-communities-v1
  view-empty               — records_cz = prázdný
  view-any                 — records_cz = union všech zásahů
  view-affil+grants        — records_cz = pouze affil + grants
  view-affil+grants+communities
  stats                    — počty zásahů dle pravidel a rozsah records_cz
  sample [table] [n]       — náhodný vzorek z tabulky (default: cz_affil_flags, 50)
  export-audit [minlen=10]
  drop-rule RULE           — smaže zásahy daného pravidla ze všech flag tabulek
  reset-flags              — smaže všechny zásahy (cz_*_flags) a nastaví prázdný records_cz
EOF
}

cmd="${1:-help}"; arg1="${2:-}"; arg2="${3:-}"

case "$cmd" in
  help|-h|--help) help ;;

  init)
    need_duckdb
    run_sql <<'SQL'
CREATE OR REPLACE MACRO cz_norm(s) AS
lower(
  regexp_replace(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
      coalesce(s,''), 
      'á','a'),'č','c'),'ď','d'),'é','e'),'ě','e'),'í','i'),'ň','n'),'ó','o'),'ř','r'),'š','s'),'ť','t'),'ú','u'),'ů','u'),'ý','y'),'ž','z'),
      'Á','a'),'Č','c'),'Ď','d'),'É','e'),'Ě','e'),'Í','i'),'Ň','n'),'Ó','o'),'Ř','r'),'Š','s'),'Ť','t'),'Ú','u'),'Ů','u'),'Ý','y'),'Ž','z'),
      'ä','a'),'ö','o'),'ü','u'),'ß','ss'),'æ','ae'),'ø','o'),'ł','l'),'ś','s'),'ż','z'),'ź','z'),
    '\s+',' ','g')
);

CREATE TABLE IF NOT EXISTS cz_affil_flags (
  record_id    BIGINT,
  author_name  VARCHAR,
  affiliation  VARCHAR,
  rule         VARCHAR,
  matched_term VARCHAR
);
CREATE TABLE IF NOT EXISTS cz_record_flags (
  record_id    BIGINT,
  source       VARCHAR,
  rule         VARCHAR,
  matched_term VARCHAR
);

CREATE OR REPLACE VIEW v_cz_any_flags AS
SELECT record_id, TRUE AS has_flag
FROM (
  SELECT record_id FROM cz_affil_flags
  UNION ALL
  SELECT record_id FROM cz_record_flags
)
GROUP BY record_id;

DROP VIEW  IF EXISTS records_cz;
DROP TABLE IF EXISTS records_cz;
CREATE VIEW records_cz AS SELECT * FROM records WHERE 1=0;

SELECT 'init_done' AS status;
SQL
    ;;

  load-institutions)
    need_duckdb
    if [[ ! -f "$INST" ]]; then echo "❌ Nenalezen $INST" >&2; exit 2; fi
    run_sql <<SQL
CREATE TABLE IF NOT EXISTS cz_institutions_raw(term VARCHAR);
DELETE FROM cz_institutions_raw;
COPY cz_institutions_raw FROM '${INST}' (FORMAT CSV, HEADER FALSE);

CREATE OR REPLACE TABLE cz_institutions AS
WITH t AS (
  SELECT regexp_replace(coalesce(term,''), '^(?i)transl:\\s*', '') AS term
  FROM cz_institutions_raw
  WHERE term IS NOT NULL AND length(trim(term))>0
)
SELECT
  term,
  cz_norm(term) AS term_norm,
  CASE WHEN length(cz_norm(term)) >= 10 THEN 'strict' ELSE 'broad' END AS quality
FROM t
WHERE length(cz_norm(term)) >= 3
GROUP BY ALL
ORDER BY term;

SELECT 'institutions_loaded' AS status, COUNT(*) AS rows FROM cz_institutions;
SQL
    ;;

  load-communities)
    need_duckdb
    if [[ ! -f "$COMMC" ]]; then echo "❌ Nenalezen $COMMC" >&2; exit 2; fi
    run_sql <<SQL
CREATE OR REPLACE TABLE cz_communities AS
SELECT
  lower(trim(identifier))                    AS identifier,
  lower(trim(identifier))                    AS id_norm,
  nullif(trim(title),'')                     AS title,
  nullif(trim(url),'')                       AS url
FROM read_csv_auto('${COMMC}', header=true)
WHERE identifier IS NOT NULL AND length(trim(identifier))>0
GROUP BY ALL
ORDER BY identifier;

SELECT 'communities_loaded' AS status, COUNT(*) AS rows FROM cz_communities;
SQL
    ;;

  rule-inst-v1)
    need_duckdb
    MINLEN="${arg1:-8}"
    run_sql <<SQL
ALTER TABLE creators ADD COLUMN IF NOT EXISTS affil_norm VARCHAR;
UPDATE creators SET affil_norm = cz_norm(affiliation) WHERE affil_norm IS NULL;

DELETE FROM cz_affil_flags WHERE rule='inst_v1';

INSERT INTO cz_affil_flags (record_id, author_name, affiliation, rule, matched_term)
SELECT c.record_id, c.author_name, c.affiliation,
       'inst_v1' AS rule, i.term AS matched_term
FROM creators c
JOIN cz_institutions i
  ON position(i.term_norm IN c.affil_norm) > 0
WHERE i.quality='strict' AND length(i.term_norm) >= ${MINLEN};

SELECT 'inst_v1' AS rule, COUNT(*) AS hits FROM cz_affil_flags WHERE rule='inst_v1';
SQL
    ;;

  rule-affkw-v1)
    need_duckdb
    PATTERN="${arg1:-'(czech|czechia|cesk|praha|prague|brno|ostrava|plzen|olomouc)'}"
    PATTERN_ESC="${PATTERN//\'/''}"
    run_sql <<SQL
ALTER TABLE creators ADD COLUMN IF NOT EXISTS affil_norm VARCHAR;
UPDATE creators SET affil_norm = cz_norm(affiliation) WHERE affil_norm IS NULL;

DELETE FROM cz_affil_flags WHERE rule='aff_keywords_v1';

INSERT INTO cz_affil_flags (record_id, author_name, affiliation, rule, matched_term)
SELECT c.record_id, c.author_name, c.affiliation,
       'aff_keywords_v1' AS rule, 'kw' AS matched_term
FROM creators c
WHERE c.affil_norm ~ '${PATTERN_ESC}';

SELECT 'aff_keywords_v1' AS rule, COUNT(*) AS hits FROM cz_affil_flags WHERE rule='aff_keywords_v1';
SQL
    ;;

  rule-grants-v1)
    need_duckdb
    PATTERN="${arg1:-'(grantova agentura ceske republiky|czech science foundation|gacr|technologicka agentura|technology agency of the czech republic|tacr|ministerstvo skolstvi|msmt)'}"
    PATTERN_ESC="${PATTERN//\'/''}"
    run_sql <<SQL
DELETE FROM cz_record_flags WHERE rule='grants_v1';

INSERT INTO cz_record_flags (record_id, source, rule, matched_term)
SELECT r.record_id, 'grant' AS source, 'grants_v1' AS rule,
       coalesce(json_extract_string(cast(g as json), '$.funder.name'),
                json_extract_string(cast(g as json), '$.funder')) AS matched_term
FROM records r, unnest(r.grants) t(g)
WHERE cz_norm(coalesce(json_extract_string(cast(g as json), '$.funder.name'),
                       json_extract_string(cast(g as json), '$.funder'), '')) ~ '${PATTERN_ESC}';

SELECT 'grants_v1' AS rule, COUNT(*) AS hits FROM cz_record_flags WHERE rule='grants_v1';
SQL
    ;;

  rule-location-v1)
    need_duckdb
    run_sql <<'SQL'
DELETE FROM cz_record_flags WHERE rule='location_v1';

INSERT INTO cz_record_flags (record_id, source, rule, matched_term)
SELECT r.record_id, 'location' AS source, 'location_v1' AS rule,
       coalesce(json_extract_string(cast(loc as json),'$.country_code'),
                json_extract_string(cast(loc as json),'$.country')) AS matched_term
FROM records r, unnest(r.locations) t(loc)
WHERE upper(coalesce(json_extract_string(cast(loc as json),'$.country_code'), '')) = 'CZ'
   OR cz_norm(coalesce(json_extract_string(cast(loc as json),'$.country'), '')) ~ '(czech|czechia|cesko|ceska republika)';

SELECT 'location_v1' AS rule, COUNT(*) AS hits FROM cz_record_flags WHERE rule='location_v1';
SQL
    ;;

  rule-text-v1)
    need_duckdb
    PATTERN="${arg1:-'(czech|czechia|cesko|ceska republika|praha|prague|brno|ostrava|plzen|olomouc)'}"
    PATTERN_ESC="${PATTERN//\'/''}"
    run_sql <<SQL
DELETE FROM cz_record_flags WHERE rule='text_v1';

INSERT INTO cz_record_flags (record_id, source, rule, matched_term)
SELECT r.record_id, 'text' AS source, 'text_v1' AS rule, 'kw' AS matched_term
FROM records r
WHERE cz_norm(coalesce(r.title,'') || ' ' || coalesce(r.description,'')) ~ '${PATTERN_ESC}';

SELECT 'text_v1' AS rule, COUNT(*) AS hits FROM cz_record_flags WHERE rule='text_v1';
SQL
    ;;

  rule-communities-v1)
    need_duckdb
    run_sql <<'SQL'
CREATE OR REPLACE VIEW v_record_communities AS
SELECT
  r.record_id,
  lower(coalesce(
    json_extract_string(cast(c as json),'$.identifier'),
    json_extract_string(cast(c as json),'$.id')
  )) AS comm_id
FROM records r, unnest(r.communities) t(c);

DELETE FROM cz_record_flags WHERE rule='communities_v1';

INSERT INTO cz_record_flags (record_id, source, rule, matched_term)
SELECT rc.record_id, 'community' AS source, 'communities_v1' AS rule, rc.comm_id AS matched_term
FROM v_record_communities rc
JOIN cz_communities zc
  ON rc.comm_id = zc.id_norm;

SELECT 'communities_v1' AS rule, COUNT(*) AS hits FROM cz_record_flags WHERE rule='communities_v1';
SQL
    ;;

  view-empty)
    need_duckdb
    run_sql <<'SQL'
DROP VIEW  IF EXISTS records_cz;
DROP TABLE IF EXISTS records_cz;
CREATE VIEW records_cz AS SELECT * FROM records WHERE 1=0;
SELECT 'records_cz empty' AS status;
SQL
    ;;

  view-any)
    need_duckdb
    run_sql <<'SQL'
CREATE OR REPLACE VIEW records_cz AS
SELECT r.*
FROM records r
JOIN v_cz_any_flags f USING (record_id);
SELECT 'records_cz = ANY FLAGS' AS status, COUNT(*) AS rows FROM records_cz;
SQL
    ;;

  view-affil+grants)
    need_duckdb
    run_sql <<'SQL'
CREATE OR REPLACE VIEW records_cz AS
SELECT r.*
FROM records r
JOIN (
  SELECT record_id FROM cz_affil_flags
  UNION
  SELECT record_id FROM cz_record_flags WHERE source IN ('grant')
) x USING (record_id);
SELECT 'records_cz = AFFIL + GRANTS' AS status, COUNT(*) AS rows FROM records_cz;
SQL
    ;;

  view-affil+grants+communities)
    need_duckdb
    run_sql <<'SQL'
CREATE OR REPLACE VIEW records_cz AS
SELECT r.*
FROM records r
JOIN (
  SELECT record_id FROM cz_affil_flags
  UNION
  SELECT record_id FROM cz_record_flags WHERE source IN ('grant','community')
) x USING (record_id);
SELECT 'records_cz = AFFIL + GRANTS + COMMUNITIES' AS status, COUNT(*) AS rows FROM records_cz;
SQL
    ;;

  stats)
    need_duckdb
    run_sql <<'SQL'
SELECT 'cz_affil_flags' AS table, COUNT(*) AS rows, COUNT(DISTINCT record_id) AS records FROM cz_affil_flags;
SELECT 'cz_record_flags' AS table, COUNT(*) AS rows, COUNT(DISTINCT record_id) AS records FROM cz_record_flags;
SELECT 'records_cz'      AS table, COUNT(*) AS rows FROM records_cz;

SELECT rule, COUNT(*) AS hits
FROM (
  SELECT rule FROM cz_affil_flags
  UNION ALL
  SELECT rule FROM cz_record_flags
)
GROUP BY rule
ORDER BY hits DESC;
SQL
    ;;

  sample)
    need_duckdb
    TABLE="${arg1:-cz_affil_flags}"; N="${arg2:-50}"
    run_sql <<SQL
SELECT * FROM ${TABLE}
ORDER BY random()
LIMIT ${N};
SQL
    ;;

  export-audit)
    need_duckdb
    mkdir -p "$OUTDIR"
    LEN="${arg1:-10}"
    run_sql <<SQL
COPY (
  SELECT * FROM cz_affil_flags
  WHERE length(cz_norm(matched_term)) < ${LEN}
) TO '${OUTDIR}/affil_short_hits.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM cz_record_flags)
TO '${OUTDIR}/record_flags_all.csv' (FORMAT CSV, HEADER TRUE);

SELECT 'exported_to' AS path, '${OUTDIR}' AS dir;
SQL
    echo "📁 Export hotový: ${OUTDIR}"
    ;;

  drop-rule)
    need_duckdb
    RULE="${arg1:-}"; [[ -z "$RULE" ]] && { echo "Použití: ./rules.sh drop-rule RULE" >&2; exit 2; }
    run_sql <<SQL
DELETE FROM cz_affil_flags  WHERE rule='${RULE}';
DELETE FROM cz_record_flags WHERE rule='${RULE}';
SELECT 'dropped' AS rule, '${RULE}' AS value;
SQL
    ;;

  reset-flags)
    need_duckdb
    run_sql <<'SQL'
DELETE FROM cz_affil_flags;
DELETE FROM cz_record_flags;
DROP VIEW  IF EXISTS records_cz;
DROP TABLE IF EXISTS records_cz;
CREATE VIEW records_cz AS SELECT * FROM records WHERE 1=0;
SELECT 'flags_cleared_and_view_empty' AS status;
SQL
    ;;

  *) help; exit 1 ;;
esac
