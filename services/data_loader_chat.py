from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import duckdb

BASE_DIR = Path(__file__).resolve().parents[1]
SUBSET_DIR = BASE_DIR / "subset_data"

PARQUETS = {
    "psu_papers": SUBSET_DIR / "psu_papers.parquet",
    "psu_references": SUBSET_DIR / "psu_references.parquet",
    "psu_paper_authors": SUBSET_DIR / "psu_paper_authors.parquet",
    "psu_authors": SUBSET_DIR / "psu_authors.parquet",
    "psu_affiliations": SUBSET_DIR / "psu_affiliations.parquet",
    "psu_paper_fields": SUBSET_DIR / "psu_paper_fields.parquet",
    "psu_fields": SUBSET_DIR / "psu_fields.parquet",
}


def get_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("PRAGMA threads=8")

    for view_name, path in PARQUETS.items():
        if path.exists():
            con.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet('{path.as_posix()}')"
            )
    return con


def list_views(con: duckdb.DuckDBPyConnection) -> List[str]:
    df = con.execute("SHOW TABLES").df()
    return [str(x) for x in df["name"].tolist()]


def describe_view(con: duckdb.DuckDBPyConnection, view_name: str) -> List[Tuple[str, str]]:
    df = con.execute(f"DESCRIBE {view_name}").df()
    return list(zip(df["column_name"].tolist(), df["column_type"].tolist()))


def schema_snapshot(con: duckdb.DuckDBPyConnection) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for v in list_views(con):
        cols = [c for c, _t in describe_view(con, v)]
        out[v] = cols
    return out


def execute_sql(con: duckdb.DuckDBPyConnection, sql: str, limit_cap: int = 3000):
    s = sql.strip().rstrip(";")
    lowered = s.lower()

    if ";" in s:
        raise ValueError("Only single SQL statement is allowed.")

    banned = [
        "insert ", "update ", "delete ", "drop ", "alter ", "create ",
        "copy ", "pragma ", "attach ", "detach "
    ]
    if any(b in lowered for b in banned):
        raise ValueError("Unsafe SQL detected.")

    if " limit " not in lowered:
        s = f"{s}\nLIMIT {limit_cap}"

    df = con.execute(s).df()
    rows = df.to_dict(orient="records")
    cols = df.columns.tolist()
    return {"sql": s, "columns": cols, "n_rows": int(len(rows)), "rows": rows}