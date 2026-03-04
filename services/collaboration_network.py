from services.data_loader import con
from datetime import datetime


def get_author_collab(university="Pennsylvania State University"):
    print(
    con.execute("""
    SELECT DISTINCT Affiliation_Name
    FROM affiliations
    WHERE LOWER(Affiliation_Name) LIKE '%penn%state%'
    LIMIT 20
    """).fetchall()
    )
    # print(
    # con.execute("""
    # SELECT DISTINCT Field_Name
    # FROM fields
    # WHERE LOWER(Field_Name) LIKE '%computer%'
    # LIMIT 50
    # """).fetchall()
    # )
    year_limit = datetime.now().year - 5

    query = f"""
    SELECT DISTINCT

        pa1.AuthorID AS source,
        pa2.AuthorID AS target,

        au1.Author_Name AS source_name,
        au2.Author_Name AS target_name,

        au1.Productivity AS source_productivity,
        au2.Productivity AS target_productivity

    FROM paper_authors pa1

    JOIN paper_authors pa2
        ON pa1.PaperID = pa2.PaperID

    JOIN papers p
        ON p.PaperID = pa1.PaperID


    -- AUTHOR TABLE
    JOIN authors au1
        ON pa1.AuthorID = au1.AuthorID

    JOIN authors au2
        ON pa2.AuthorID = au2.AuthorID


    -- AFFILIATION FILTER
    JOIN affiliations aff1
        ON pa1.AffiliationID = aff1.AffiliationID

    JOIN affiliations aff2
        ON pa2.AffiliationID = aff2.AffiliationID


    -- FIELD FILTER
    JOIN paper_fields pf
        ON p.PaperID = pf.PaperID

    JOIN fields f
        ON pf.FieldID = f.FieldID


    WHERE
        pa1.AuthorID != pa2.AuthorID
        AND p.Year >= {year_limit}

        AND EXISTS (
            SELECT 1
            FROM paper_fields pf2
            JOIN fields f2
                ON pf2.FieldID = f2.FieldID
            WHERE pf2.PaperID = p.PaperID
            AND LOWER(f2.Field_Name) LIKE '%computer%'
        )

        AND (
            LOWER(aff1.Affiliation_Name) LIKE '%penn%state%'
            AND
            LOWER(aff2.Affiliation_Name) LIKE '%penn%state%'
        )

    LIMIT 3000
    """
    df = con.execute(query).df()

    nodes = {}

    for _, r in df.iterrows():

        nodes[r.source] = {
            "id": str(r.source),
            "name": r.source_name,
            "productivity": r.source_productivity
        }

        nodes[r.target] = {
            "id": str(r.target),
            "name": r.target_name,
            "productivity": r.target_productivity
        }

    links = [
        {
            "source": str(r.source),
            "target": str(r.target)
        }
        for _, r in df.iterrows()
    ]

    return {
        "nodes": list(nodes.values()),
        "links": links
    }