import os
import duckdb
from services.data_loader import register_tables, con

OUTPUT_DIR = "subset_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============ Config ============
UNIV_KEYWORD = "%pennsylvania state%"
YEAR_MIN = 2017
YEAR_MAX = 2021
PAPER_LIMIT = 6000  # 你现在用的 6000；想全量就设为 None 并删掉 LIMIT

print("Registering full SciSciNet tables...")
register_tables()

# 可选：性能小优化
con.execute("PRAGMA threads=8")
con.execute("PRAGMA enable_progress_bar=true")

print("Building PSU subset...")

# -------------------------
# 1) PSU papers (PaperID, Year)
# -------------------------
limit_clause = f"LIMIT {PAPER_LIMIT}" if PAPER_LIMIT else ""

con.execute(f"""
CREATE OR REPLACE TABLE psu_papers AS
SELECT DISTINCT p.PaperID, p.Year
FROM papers p
JOIN paper_authors pa ON p.PaperID = pa.PaperID
JOIN affiliations a ON pa.AffiliationID = a.AffiliationID
WHERE LOWER(a.Affiliation_Name) LIKE '{UNIV_KEYWORD}'
  AND p.Year >= {YEAR_MIN}
  AND p.Year <= {YEAR_MAX}
{limit_clause}
""")

# -------------------------
# 2) References where citing is PSU paper
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_references AS
SELECT r.Citing_PaperID, r.Cited_PaperID
FROM paper_references r
JOIN psu_papers p ON r.Citing_PaperID = p.PaperID
""")

# -------------------------
# 3) Paper-Author-Affiliation links for PSU papers
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_paper_authors AS
SELECT
  pa.PaperID,
  pa.AuthorID,
  pa.AffiliationID,
  pa.AuthorSequenceNumber
FROM paper_authors pa
JOIN psu_papers p ON pa.PaperID = p.PaperID
""")

# -------------------------
# 4) Authors table subset
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_authors AS
SELECT DISTINCT a.*
FROM authors a
JOIN (SELECT DISTINCT AuthorID FROM psu_paper_authors) x
  ON a.AuthorID = x.AuthorID
""")

# -------------------------
# 5) Affiliations table subset (only those appearing in PSU paper_authors)
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_affiliations AS
SELECT DISTINCT af.*
FROM affiliations af
JOIN (SELECT DISTINCT AffiliationID FROM psu_paper_authors WHERE AffiliationID IS NOT NULL) x
  ON af.AffiliationID = x.AffiliationID
""")

# -------------------------
# 6) Paper-Field links for PSU papers
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_paper_fields AS
SELECT pf.PaperID, pf.FieldID
FROM paper_fields pf
JOIN psu_papers p ON pf.PaperID = p.PaperID
""")

# -------------------------
# 7) Fields table (small; either full or filtered)
# 方案A：只保留 PSU 用到的 fields（推荐更干净）
# -------------------------
con.execute("""
CREATE OR REPLACE TABLE psu_fields AS
SELECT DISTINCT f.*
FROM fields f
JOIN (SELECT DISTINCT FieldID FROM psu_paper_fields) x
  ON f.FieldID = x.FieldID
""")

print("Exporting to parquet...")

def export_parquet(table_name: str, out_name: str):
    con.execute(f"""
    COPY {table_name}
    TO '{OUTPUT_DIR}/{out_name}.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

export_parquet("psu_papers", "psu_papers")
export_parquet("psu_references", "psu_references")
export_parquet("psu_paper_authors", "psu_paper_authors")
export_parquet("psu_authors", "psu_authors")
export_parquet("psu_affiliations", "psu_affiliations")
export_parquet("psu_paper_fields", "psu_paper_fields")
export_parquet("psu_fields", "psu_fields")

print("Done.")