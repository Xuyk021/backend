from services.data_loader import con
from datetime import datetime


def get_citation_network(university="Pennsylvania State University"):

    year_limit = datetime.now().year - 5

    query = f"""
    SELECT DISTINCT
        r.Citing_PaperID,
        r.Cited_PaperID,

        p1.Year AS citing_year,
        p1.Citation_Count AS citing_citations,

        p2.Year AS cited_year,
        p2.Citation_Count AS cited_citations

    FROM paper_references r

    JOIN papers p1
        ON r.Citing_PaperID = p1.PaperID

    JOIN papers p2
        ON r.Cited_PaperID = p2.PaperID


    -- ===============================
    -- FIELD FILTER (Computer Science)
    -- ===============================

    JOIN paper_fields pf1
        ON p1.PaperID = pf1.PaperID

    JOIN fields f1
        ON pf1.FieldID = f1.FieldID

    JOIN paper_fields pf2
        ON p2.PaperID = pf2.PaperID

    JOIN fields f2
        ON pf2.FieldID = f2.FieldID


    -- ===============================
    -- UNIVERSITY FILTER (PSU)
    -- ===============================

    JOIN paper_authors pa1
        ON p1.PaperID = pa1.PaperID

    JOIN affiliations aff1
        ON pa1.AffiliationID = aff1.AffiliationID

    JOIN paper_authors pa2
        ON p2.PaperID = pa2.PaperID

    JOIN affiliations aff2
        ON pa2.AffiliationID = aff2.AffiliationID


    WHERE

        -- BOTH papers recent
        p1.Year >= {year_limit}
        AND p2.Year >= {year_limit}

        -- BOTH CS
        AND LOWER(f1.Field_Name) LIKE '%computer%'
        AND LOWER(f2.Field_Name) LIKE '%computer%'

        -- BOTH PSU
        AND LOWER(aff1.Affiliation_Name)
            LIKE '%pennsylvania state%'

        AND LOWER(aff2.Affiliation_Name)
            LIKE '%pennsylvania state%'

    LIMIT 1500
    """

    df = con.execute(query).df()

    nodes = {}

    for _, row in df.iterrows():

        nodes[row.Citing_PaperID] = {
            "id": str(row.Citing_PaperID),
            "year": row.citing_year,
            "citations": row.citing_citations
        }

        nodes[row.Cited_PaperID] = {
            "id": str(row.Cited_PaperID),
            "year": row.cited_year,
            "citations": row.cited_citations
        }

    links = [
        {
            "source": str(r.Citing_PaperID),
            "target": str(r.Cited_PaperID),
            "type": "citation"
        }
        for _, r in df.iterrows()
    ]

    return {
        "nodes": list(nodes.values()),
        "links": links
    }