from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

BASE = Path(__file__).parent
papers_path = BASE / "subset_data" / "psu_papers.parquet"
refs_path   = BASE / "subset_data" / "psu_references.parquet"

def quick_parquet_info(path: Path, name: str):
    print(f"\n==== {name} ====")
    print("Path:", path)
    if not path.exists():
        print("❌ File not found")
        return None

    pf = pq.ParquetFile(path)
    print("✅ Read OK")
    print("Row groups:", pf.num_row_groups)
    print("Rows:", pf.metadata.num_rows)
    print("Columns:", pf.schema.names)

    
    df_head = pf.read_row_group(0).to_pandas().head(5) if pf.num_row_groups > 0 else pd.DataFrame()
    print("\nHead(5):")
    print(df_head)

    return pf

def main():
    papers_pf = quick_parquet_info(papers_path, "psu_papers.parquet")
    refs_pf   = quick_parquet_info(refs_path,   "psu_references.parquet")

    
    if papers_pf and refs_pf:
        papers = pd.read_parquet(papers_path)
        refs   = pd.read_parquet(refs_path)

        print("\n==== Basic counts ====")
        print("papers rows:", len(papers))
        print("refs rows:", len(refs))

        print("\n==== Null check (top suspicious columns) ====")
        for col in ["PaperID", "Year", "Field", "AffiliationName"]:
            if col in papers.columns:
                print(f"papers[{col}] nulls:", papers[col].isna().sum())

        for col in ["Citing_PaperID", "Cited_PaperID"]:
            if col in refs.columns:
                print(f"refs[{col}] nulls:", refs[col].isna().sum())

        
        if "PaperID" in papers.columns:
            dup = papers["PaperID"].duplicated().sum()
            print("\nPaperID duplicates:", dup)

        
        if "PaperID" in papers.columns and {"Citing_PaperID","Cited_PaperID"}.issubset(refs.columns):
            paper_ids = set(papers["PaperID"].astype(str))
            citing_in = refs["Citing_PaperID"].astype(str).isin(paper_ids).mean()
            cited_in  = refs["Cited_PaperID"].astype(str).isin(paper_ids).mean()
            print("\n==== Referential integrity (ratio) ====")
            print(f"citing_id in papers: {citing_in:.3f}")
            print(f"cited_id  in papers: {cited_in:.3f}")
            print("（正常情况：citing_in 通常很高；cited_in 可能较低，因为引用对象可能在 PSU 外）")

       
        if "Year" in papers.columns:
            yr = papers["Year"].dropna()
            if len(yr) > 0:
                print("\n==== Year range ====")
                print("min year:", int(yr.min()))
                print("max year:", int(yr.max()))
                print("top years:", yr.value_counts().head(10).to_dict())

if __name__ == "__main__":
    main()