import os
from datetime import date, datetime
import duckdb
import streamlit as st
import pandas as pd  # na dataframe a isna/notna

st.set_page_config(page_title="Zenodo Datasets Browser", layout="wide")

# ------------------------- Helpers -------------------------

def to_pydate(x):
    """Bezpečně převede libovolný vstup (duckdb date, datetime, str, None) na datetime.date (nebo None)."""
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        try:
            return date.fromisoformat(x[:10])
        except Exception:
            return None
    try:
        s = str(x)
        return date.fromisoformat(s[:10])
    except Exception:
        return None

MIN_SAFE = date(1600, 1, 1)
MAX_SAFE = date(2100, 12, 31)

def clamp_d(d: date | None, lo=MIN_SAFE, hi=MAX_SAFE):
    if d is None:
        return None
    if d < lo:
        return lo
    if d > hi:
        return hi
    return d

def nonempty_str(x):
    """Vrátí ořezaný str, nebo None při None/pd.NA/NaN/prázdném stringu."""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    return s if s else None

# ------------------------- Sidebar: zdroj dat -------------------------

st.sidebar.title("Data source")
mode = st.sidebar.radio("Zdroj", ["DuckDB DB", "JSONL (raw)"], index=0)

default_db = "zenodo_dump/zenodo.duckdb"
default_jsonl = "zenodo_dump/raw/records.jsonl"

if mode == "DuckDB DB":
    db_path = st.sidebar.text_input("Cesta k .duckdb", value=default_db)
    st.sidebar.caption("Očekávám tabulky: records, creators (vytvoří je harvest/postprocessing).")
else:
    jsonl_path = st.sidebar.text_input("Cesta k .jsonl", value=default_jsonl)
    st.sidebar.caption("Čte JSONL přes DuckDB JSON extension (pomalejší).")

@st.cache_resource(show_spinner=True)
def connect_duckdb(db_path: str):
    return duckdb.connect(db_path, read_only=False)

@st.cache_resource(show_spinner=True)
def connect_json(jsonl_path: str):
    con = duckdb.connect(":memory:")
    con.execute("INSTALL json; LOAD json;")
    con.execute("CREATE OR REPLACE VIEW raw AS SELECT * FROM read_json_auto(?);", [jsonl_path])
    return con

st.title("Zenodo datasets – přehled a detail")

# ------------------------- Připojení -------------------------

con = None
using_db = (mode == "DuckDB DB")

if using_db:
    if not os.path.exists(default_db) and db_path == default_db:
        st.info("Soubor zenodo_dump/zenodo.duckdb zatím nevidím. Můžeš použít „JSONL (raw)“ nebo nejdřív doběhnout postprocessing.")
    try:
        con = connect_duckdb(db_path)
        have = set(con.execute("SHOW TABLES").fetchnumpy()["name"])
        if not {"records", "creators"}.issubset(have):
            st.warning("V DB chybí tabulky 'records' a/nebo 'creators'. Přepni na JSONL, nebo nech doběhnout postprocessing.")
    except Exception as e:
        st.error(f"Nepodařilo se otevřít DuckDB: {e}")
else:
    if not os.path.exists(jsonl_path):
        st.error("Soubor JSONL neexistuje.")
    else:
        try:
            con = connect_json(jsonl_path)
        except Exception as e:
            st.error(f"Nepodařilo se otevřít JSONL: {e}")

if con is None:
    st.stop()

# ------------------------- Společné filtry -------------------------

q = st.sidebar.text_input("Hledat (název/popisek)")
aff_q = st.sidebar.text_input("Afiliace obsahuje")
page_size = st.sidebar.selectbox("Počet na stránku", [25, 50, 100], index=1)
page = st.sidebar.number_input("Stránka (1…)", min_value=1, value=1, step=1)

langs = []
cz_only = False
date_from = None
date_to = None

# ------------------------- DuckDB (flattenované tabulky) -------------------------

if using_db:
    # jazyky
    try:
        langs_all = [r[0] for r in con.execute("SELECT DISTINCT language FROM records WHERE language IS NOT NULL ORDER BY 1").fetchall()]
    except Exception:
        langs_all = []
    langs = st.sidebar.multiselect("Jazyky", langs_all)

    # rozmezí dat
    try:
        dmin, dmax = con.execute("""
            SELECT MIN(try_cast(publication_date AS DATE)),
                   MAX(try_cast(publication_date AS DATE))
            FROM records
        """).fetchone()
    except Exception:
        dmin = dmax = None

    dmin = clamp_d(to_pydate(dmin)) or MIN_SAFE
    dmax = clamp_d(to_pydate(dmax)) or MAX_SAFE

    df_sel = st.sidebar.date_input("Datum od", value=dmin)
    dt_sel = st.sidebar.date_input("Datum do", value=dmax)
    date_from = df_sel if isinstance(df_sel, date) else to_pydate(df_sel)
    date_to   = dt_sel if isinstance(dt_sel, date) else to_pydate(dt_sel)

    cz_only = st.sidebar.checkbox("Pouze záznamy se stopou ČR", value=False)

    # WHERE
    where = ["1=1"]
    params = []

    if q:
        where.append("lower(coalesce(title,'') || ' ' || coalesce(description,'')) LIKE ?")
        params.append(f"%{q.lower()}%")

    if langs:
        where.append("coalesce(language,'') IN (" + ",".join(["?"]*len(langs)) + ")")
        params.extend(langs)

    if date_from and date_to:
        where.append("try_cast(publication_date AS DATE) BETWEEN ? AND ?")
        params.extend([str(date_from), str(date_to)])

    if cz_only:
        where.append("(has_cz_text_hit OR has_cz_affil_any_author OR COALESCE(has_affil_match, FALSE))")

    if aff_q:
        where.append("""EXISTS (
            SELECT 1 FROM creators c
            WHERE c.record_id = records.record_id
              AND lower(coalesce(c.affiliation,'')) LIKE ?
        )""")
        params.append(f"%{aff_q.lower()}%")

    where_sql = " AND ".join(where)

    total = con.execute(f"SELECT COUNT(*) FROM records WHERE {where_sql}", params).fetchone()[0]
    st.caption(f"Počet záznamů (po filtrech): {total:,}")

    offset = (page-1)*page_size
    rows = con.execute(f"""
        SELECT record_id, title, publication_date, language, doi, links_html
        FROM records
        WHERE {where_sql}
        ORDER BY try_cast(publication_date AS DATE) DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [page_size, offset]).fetchdf()

    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.subheader("Detaily vybraných záznamů")
    max_details = st.slider("Kolik detailů vykreslit (z aktuální stránky)", 1, min(len(rows), 10), min(len(rows), 5))
    for _, r in rows.head(max_details).iterrows():
        title = nonempty_str(r["title"]) or "(bez názvu)"
        with st.expander(f"[{r['record_id']}] {title}"):
            doi = nonempty_str(r["doi"])
            if doi:
                st.write("**DOI:**", doi)
            link = nonempty_str(r["links_html"])
            if link:
                st.write("**Zenodo:**", link)
            st.write("**Publication date:**", nonempty_str(r["publication_date"]) or "—")
            st.write("**Language:**", nonempty_str(r["language"]) or "—")

            df_auth = con.execute("""
                SELECT author_name, affiliation, orcid
                FROM creators
                WHERE record_id = ?
                ORDER BY author_name NULLS LAST
            """, [r["record_id"]]).fetchdf()
            if not df_auth.empty:
                st.markdown("**Autoři & afiliace**")
                st.dataframe(df_auth, hide_index=True, use_container_width=True)
            else:
                st.caption("Bez záznamu autorů.")

# ------------------------- JSONL (raw) přes DuckDB JSON -------------------------

else:
    con.execute("""
        CREATE OR REPLACE VIEW rec AS
        SELECT
            id AS record_id,
            doi,
            conceptdoi,
            conceptrecid,
            created,
            updated,
            (links).html AS links_html,
            (metadata).title AS title,
            (metadata).description AS description,
            (metadata).language AS language,
            COALESCE((metadata).publication_date, (metadata).publicationdate) AS publication_date
        FROM raw;
    """)

    try:
        langs_all = [r[0] for r in con.execute("SELECT DISTINCT language FROM rec WHERE language IS NOT NULL ORDER BY 1").fetchall()]
    except Exception:
        langs_all = []
    langs = st.sidebar.multiselect("Jazyky", langs_all)

    dmin, dmax = con.execute("""
        SELECT MIN(
                 COALESCE(try_strptime(publication_date,'%Y-%m-%d')::DATE,
                          DATE(try_cast(created AS TIMESTAMP)),
                          DATE(try_cast(updated AS TIMESTAMP)))
               ),
               MAX(
                 COALESCE(try_strptime(publication_date,'%Y-%m-%d')::DATE,
                          DATE(try_cast(created AS TIMESTAMP)),
                          DATE(try_cast(updated AS TIMESTAMP)))
               )
        FROM rec
    """).fetchone()

    dmin = clamp_d(to_pydate(dmin)) or MIN_SAFE
    dmax = clamp_d(to_pydate(dmax)) or MAX_SAFE

    df_sel = st.sidebar.date_input("Datum od", value=dmin)
    dt_sel = st.sidebar.date_input("Datum do", value=dmax)
    date_from = df_sel if isinstance(df_sel, date) else to_pydate(df_sel)
    date_to   = dt_sel if isinstance(dt_sel, date) else to_pydate(dt_sel)

    where = ["1=1"]
    params = []
    if q:
        where.append("lower(coalesce(title,'') || ' ' || coalesce(description,'')) LIKE ?")
        params.append(f"%{q.lower()}%")
    if langs:
        where.append("coalesce(language,'') IN (" + ",".join(["?"]*len(langs)) + ")")
        params.extend(langs)
    if date_from and date_to:
        where.append("""
            COALESCE(try_strptime(publication_date,'%Y-%m-%d')::DATE,
                     DATE(try_cast(created AS TIMESTAMP)),
                     DATE(try_cast(updated AS TIMESTAMP))) BETWEEN ? AND ?
        """)
        params.extend([str(date_from), str(date_to)])
    if aff_q:
        where.append("""
            EXISTS (
              SELECT 1
              FROM raw r2, UNNEST((r2.metadata).creators) AS c
              WHERE r2.id = rec.record_id
                AND lower(
                      COALESCE(c.affiliation,
                               CASE WHEN array_length(c.affiliations) > 0 THEN (c.affiliations[1]).name ELSE '' END, '')
                    ) LIKE ?
            )
        """)
        params.append(f"%{aff_q.lower()}%")

    where_sql = " AND ".join(where)

    total = con.execute(f"SELECT COUNT(*) FROM rec WHERE {where_sql}", params).fetchone()[0]
    st.caption(f"Počet záznamů (po filtrech): {total:,}")

    offset = (page-1)*page_size
    rows = con.execute(f"""
        SELECT record_id, title, publication_date, language, doi, links_html
        FROM rec
        WHERE {where_sql}
        ORDER BY COALESCE(try_strptime(publication_date,'%Y-%m-%d')::DATE,
                          DATE(try_cast(created AS TIMESTAMP)),
                          DATE(try_cast(updated AS TIMESTAMP))) DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [page_size, offset]).fetchdf()

    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.subheader("Detaily vybraných záznamů")
    max_details = st.slider("Kolik detailů vykreslit (z aktuální stránky)", 1, min(len(rows), 10), min(len(rows), 5))
    for _, r in rows.head(max_details).iterrows():
        title = nonempty_str(r["title"]) or "(bez názvu)"
        with st.expander(f"[{r['record_id']}] {title}"):
            doi = nonempty_str(r["doi"])
            if doi:
                st.write("**DOI:**", doi)
            link = nonempty_str(r["links_html"])
            if link:
                st.write("**Zenodo:**", link)
            st.write("**Publication date:**", nonempty_str(r["publication_date"]) or "—")
            st.write("**Language:**", nonempty_str(r["language"]) or "—")

            df_auth = con.execute("""
                SELECT
                  COALESCE(c.name, (c.person_or_org).name) AS author_name,
                  COALESCE(c.affiliation,
                           CASE WHEN array_length(c.affiliations) > 0 THEN (c.affiliations[1]).name ELSE NULL END) AS affiliation,
                  c.orcid AS orcid
                FROM raw r, UNNEST((r.metadata).creators) AS c
                WHERE r.id = ?
                ORDER BY author_name NULLS LAST
            """, [r["record_id"]]).fetchdf()
            if not df_auth.empty:
                st.markdown("**Autoři & afiliace**")
                st.dataframe(df_auth, hide_index=True, use_container_width=True)

