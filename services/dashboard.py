from services.data_loader import con
from datetime import datetime

def get_cs_timeline(university="pennsylvania state"):

    # current_year = datetime.now().year
    # start_year = current_year - 10
    current_year = 2021
    start_year = 2011
    print(f"Getting CS timeline for {university} from {start_year} to {current_year}...")

    query = f"""
    SELECT
        p.Year,
        COUNT(DISTINCT p.PaperID) AS paper_count
    FROM papers p

    JOIN paper_fields pf
        ON p.PaperID = pf.PaperID

    JOIN paper_authors pa
        ON p.PaperID = pa.PaperID

    JOIN affiliations aff
        ON pa.AffiliationID = aff.AffiliationID

    WHERE
        p.Year >= {start_year}
        AND p.Year <= {current_year}
        AND pf.FieldID = 41008148
        AND LOWER(aff.Affiliation_Name)
            LIKE '%pennsylvania%state%'

    GROUP BY p.Year
    ORDER BY p.Year
    """

    return con.execute(query).fetchall()


def get_patent_histogram(year=None, university="pennsylvania state"):

    print(con.execute("""
    SELECT COUNT(*)
    FROM patents
    """).fetchall())
    print(con.execute("""
    SELECT COUNT(*)
    FROM papers p
    JOIN paper_fields pf
        ON p.PaperID = pf.PaperID
    JOIN paper_authors pa
        ON p.PaperID = pa.PaperID
    JOIN affiliations aff
        ON pa.AffiliationID = aff.AffiliationID
    JOIN patents pt
        ON p.PaperID = pt.PaperID
    WHERE
        pf.FieldID = 41008148
        AND LOWER(aff.Affiliation_Name)
            LIKE '%pennsylvania%state%'
    """).fetchall())

    year_filter = f"AND p.Year = {year}" if year else ""

    query = f"""
    SELECT
        p.PaperID,
        COUNT(pt.PatentID) AS Patent_Count

    FROM papers p

    LEFT JOIN patents pt
        ON p.PaperID = pt.PaperID

    JOIN paper_fields pf
        ON p.PaperID = pf.PaperID

    JOIN paper_authors pa
        ON p.PaperID = pa.PaperID

    JOIN affiliations aff
        ON pa.AffiliationID = aff.AffiliationID

    WHERE
        pf.FieldID = 41008148
        AND LOWER(aff.Affiliation_Name)
            LIKE '%pennsylvania%state%'
        {year_filter}

    GROUP BY p.PaperID
    """

    df = con.execute(query).df()

    return df["Patent_Count"].tolist()