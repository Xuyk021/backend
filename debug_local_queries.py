# debug_local_queries.py
from pathlib import Path
import traceback

from services.data_loader_chat import get_con
from services.tools import build_sql


BASE = Path(__file__).parent
SUBSET = BASE / "subset_data"

def exists(p: Path) -> str:
    return "✅" if p.exists() else "❌"

def safe_exec(con, sql: str, title: str):
    print("\n" + "=" * 80)
    print(f"[TEST] {title}")
    print("- SQL:")
    print(sql.strip())
    try:
        df = con.execute(sql).df()
        print(f"- ✅ OK. rows={len(df)} cols={len(df.columns)}")
        print(df.head(10).to_string(index=False))
    except Exception as e:
        print("- ❌ FAILED:")
        print(type(e).__name__, str(e))
        # 打印更详细的栈，方便定位 binder / catalog 错误
        traceback.print_exc(limit=2)

def main():
    print("=== Step 1: check subset_data parquet existence ===")
    # 这些名字必须和 data_loader.get_con() 里一致
    required = [
        "psu_papers.parquet",
        "psu_paper_fields.parquet",
        "psu_fields.parquet",
        "psu_paper_authors.parquet",
        "psu_authors.parquet",
    ]
    for name in required:
        p = SUBSET / name
        print(f"{exists(p)} {p}")

    print("\n=== Step 2: create duckdb connection and list views ===")
    con = get_con()
    # 列出当前 schema 里有什么（view / table）
    safe_exec(con, "SHOW ALL TABLES;", "SHOW ALL TABLES")

    print("\n=== Step 3: DESCRIBE key views (to verify column names) ===")
    for v in ["psu_papers", "psu_paper_fields", "psu_fields", "psu_paper_authors", "psu_authors"]:
        safe_exec(con, f"DESCRIBE {v};", f"DESCRIBE {v}")

    print("\n=== Step 4: run the exact SQL your app uses (no OpenAI) ===")
    for intent in ["paper_count_by_year", "paper_count_by_field", "paper_count_by_author"]:
        sql = build_sql(intent, {})
        safe_exec(con, sql, f"build_sql('{intent}')")

if __name__ == "__main__":
    main()