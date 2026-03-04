from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from services.data_loader_chat import get_con, schema_snapshot, execute_sql


SUPPORTED_INTENTS = [
    "papers_by_year",
    "papers_by_field",
    "papers_by_author",
    "papers_by_affiliation",
    "citations_by_year",
    "top_cited_psu_papers",
]


@tool("get_schema")
def get_schema() -> Dict[str, List[str]]:
    """Return available DuckDB views and their columns for the subset parquet database."""
    con = get_con()
    return schema_snapshot(con)


@tool("run_sql")
def run_sql(sql: str) -> Dict[str, Any]:
    """Execute a single read-only DuckDB SQL query against subset views and return rows and columns."""
    con = get_con()
    return execute_sql(con, sql)


def build_sql(intent: str, year_min: Optional[int], year_max: Optional[int], top_n: int) -> str:
    """Build a Vega-Lite v5 spec from query rows using deterministic templates for the given intent."""
    top_n = int(top_n or 20)

    def year_filter(prefix: str = "p") -> str:
        parts = []
        if year_min is not None:
            parts.append(f"{prefix}.Year >= {int(year_min)}")
        if year_max is not None:
            parts.append(f"{prefix}.Year <= {int(year_max)}")
        return (" AND " + " AND ".join(parts)) if parts else ""

    if intent == "papers_by_year":
        return f"""
SELECT p.Year AS Year, COUNT(*) AS count
FROM psu_papers p
WHERE p.Year IS NOT NULL{year_filter("p")}
GROUP BY p.Year
ORDER BY p.Year
""".strip()

    if intent == "citations_by_year":
        return f"""
SELECT p.Year AS Year, COUNT(*) AS count
FROM psu_references r
JOIN psu_papers p ON p.PaperID = r.Citing_PaperID
WHERE p.Year IS NOT NULL{year_filter("p")}
GROUP BY p.Year
ORDER BY p.Year
""".strip()

    if intent == "papers_by_field":
        return f"""
SELECT f.Field_Name AS Field, COUNT(DISTINCT p.PaperID) AS count
FROM psu_papers p
JOIN psu_paper_fields pf ON pf.PaperID = p.PaperID
JOIN psu_fields f ON f.FieldID = pf.FieldID
WHERE f.Field_Name IS NOT NULL AND f.Field_Name <> ''{year_filter("p")}
GROUP BY f.Field_Name
ORDER BY count DESC

""".strip()

    if intent == "papers_by_author":
        return f"""
SELECT au.Author_Name AS Author, COUNT(DISTINCT p.PaperID) AS count
FROM psu_papers p
JOIN psu_paper_authors pa ON pa.PaperID = p.PaperID
JOIN psu_authors au ON au.AuthorID = pa.AuthorID
WHERE au.Author_Name IS NOT NULL AND au.Author_Name <> ''{year_filter("p")}
GROUP BY au.Author_Name
ORDER BY count DESC
""".strip()

    if intent == "papers_by_affiliation":
        return f"""
SELECT a.Affiliation_Name AS Affiliation, COUNT(DISTINCT p.PaperID) AS count
FROM psu_papers p
JOIN psu_paper_authors pa ON pa.PaperID = p.PaperID
JOIN psu_affiliations a ON a.AffiliationID = pa.AffiliationID
WHERE a.Affiliation_Name IS NOT NULL AND a.Affiliation_Name <> ''{year_filter("p")}
GROUP BY a.Affiliation_Name
ORDER BY count DESC

""".strip()

    if intent == "top_cited_psu_papers":
        return f"""
SELECT r.Cited_PaperID AS PaperID, COUNT(*) AS cited_count
FROM psu_references r
WHERE r.Cited_PaperID IN (SELECT PaperID FROM psu_papers)
GROUP BY r.Cited_PaperID
ORDER BY cited_count DESC

""".strip()

    raise ValueError(f"Unsupported intent: {intent}")


@tool("build_vega_spec")
def build_vega_spec(intent: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a Vega-Lite v5 spec from query rows using deterministic templates for the given intent."""
    if intent in ("papers_by_year", "citations_by_year"):
        return _bar_year(rows, x_field="Year", y_field="count")

    if intent == "papers_by_field":
        return _bar_horizontal(rows, y_field="Field", x_field="count", title="Papers by Field")

    if intent == "papers_by_author":
        return _bar_horizontal(rows, y_field="Author", x_field="count", title="Papers by Author")

    if intent == "papers_by_affiliation":
        return _bar_horizontal(rows, y_field="Affiliation", x_field="count", title="Papers by Affiliation")

    if intent == "top_cited_psu_papers":
        return _bar_horizontal(rows, y_field="PaperID", x_field="cited_count", title="Top Cited PSU Papers")

    raise ValueError(f"No spec template for intent: {intent}")


@tool("validate_vega_spec")
def validate_vega_spec(spec: Dict[str, Any], data_columns: List[str]) -> Dict[str, Any]:
    """Validate Vega-Lite spec fields and required interactions (hover + selection) against data columns."""
    issues: List[str] = []

    if not isinstance(spec, dict):
        return {"ok": False, "issues": ["Spec is not a dict"]}

    if "data" not in spec or "values" not in spec["data"]:
        issues.append("Spec.data.values missing")

    enc = spec.get("encoding", {})
    for axis in ("x", "y"):
        if axis in enc:
            f = enc[axis].get("field")
            if f and f not in data_columns:
                issues.append(f"encoding.{axis}.field '{f}' not in data columns")

    params = spec.get("params", [])
    has_hover = any(p.get("name") == "hover" for p in params)
    has_pick = any(p.get("name") == "pick" for p in params)
    if not (has_hover and has_pick):
        issues.append("Missing required interactions (hover + selection)")

    return {"ok": len(issues) == 0, "issues": issues}


def _bar_year(rows: List[Dict[str, Any]], x_field: str, y_field: str) -> Dict[str, Any]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": rows},
        "params": [
            {"name": "pick", "select": {"type": "point", "fields": [x_field]}},
            {"name": "hover", "select": {"type": "point", "on": "mouseover", "clear": "mouseout"}},
        ],
        "mark": {"type": "bar"},
        "encoding": {
            "x": {"field": x_field, "type": "ordinal", "sort": "ascending", "title": x_field},
            "y": {"field": y_field, "type": "quantitative", "title": y_field},
            "tooltip": [{"field": x_field, "type": "ordinal"}, {"field": y_field, "type": "quantitative"}],
            "opacity": {"condition": {"param": "pick", "value": 1}, "value": 0.6},
        },
        "config": {"view": {"stroke": "transparent"}},
    }


def _bar_horizontal(rows: List[Dict[str, Any]], y_field: str, x_field: str, title: str) -> Dict[str, Any]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": title,
        "data": {"values": rows},
        "params": [
            {"name": "pick", "select": {"type": "point", "fields": [y_field]}},
            {"name": "hover", "select": {"type": "point", "on": "mouseover", "clear": "mouseout"}},
        ],
        "mark": {"type": "bar"},
        "encoding": {
            "y": {"field": y_field, "type": "nominal", "sort": "-x", "title": y_field},
            "x": {"field": x_field, "type": "quantitative", "title": x_field},
            "tooltip": [{"field": y_field, "type": "nominal"}, {"field": x_field, "type": "quantitative"}],
            "opacity": {"condition": {"param": "pick", "value": 1}, "value": 0.6},
        },
        "config": {"view": {"stroke": "transparent"}},
    }