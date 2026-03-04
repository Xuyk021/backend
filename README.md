## Backend Overview
Flask backend for PSU-focused bibliometrics. It serves:
- Graph analytics endpoints (citation network, author collaboration network, timeline, patent histogram) from TSV data.
- A chat/analytics endpoint backed by LangGraph + OpenAI that queries a DuckDB subset in Parquet and returns Vega-Lite specs.

## Project Layout
- `app.py`: Graph API server (TSV-backed).
- `app_chat.py`: Chat API server (Parquet subset + LLM).
- `routes/graph_routes.py`: Graph endpoints.
- `routes/chat.py`: Chat endpoint.
- `services/data_loader.py`: Registers TSV views in DuckDB.
- `services/data_loader_chat.py`: Registers Parquet subset views in DuckDB.
- `services/*_network.py`: Graph queries for citations and author collaborations.
- `services/tools.py`: DuckDB tools + Vega-Lite spec builders for chat.
- `services/agent_graph.py`: LangGraph router/analyst/viz/critic pipeline.

## Data Dependencies
TSV sources are loaded from `C:/PSU/FSU/Data` (configured in `config.py`):
- `SciSciNet_Papers.tsv`
- `SciSciNet_Authors.tsv`
- `SciSciNet_PaperAuthorAffiliations.tsv`
- `SciSciNet_PaperReferences.tsv`
- `SciSciNet_PaperFields.tsv`
- `SciSciNet_Fields.tsv`
- `SciSciNet_Affiliations.tsv`
- `SciSciNet_Link_Patents.tsv`

Chat uses Parquet subset files in `subset_data/`:
- `psu_papers.parquet`, `psu_references.parquet`, `psu_paper_authors.parquet`,
  `psu_authors.parquet`, `psu_affiliations.parquet`, `psu_paper_fields.parquet`, `psu_fields.parquet`

## Setup
Prereqs: Python 3.12+

1. Create and activate a venv (if needed).
2. Install dependencies:
```bash
pip install -e .
```
3. Set OpenAI key for chat (in your shell or `.env`):
```bash
set OPENAI_API_KEY=your_key_here
```
4. Verify data paths in `config.py` if your TSVs live elsewhere.

## Run
Graph API:
```bash
python app.py
```

Chat API:
```bash
python app_chat.py
```

Default ports:
- `app.py`: Flask default `5000` (debug on).
- `app_chat.py`: `127.0.0.1:5000` (debug on).

## API Endpoints
Graph endpoints (`app.py`):
- `GET /api/citation-network?university=...`
  - Returns `{ nodes: [{id, year, citations}, ...], links: [{source, target, type}, ...] }`
- `GET /api/author-network?university=...`
  - Returns `{ nodes: [{id, name, productivity}, ...], links: [{source, target}, ...] }`
- `GET /api/timeline`
  - Returns `[ {Year, paper_count}, ... ]`
- `GET /api/patent-histogram?year=...`
  - Returns `[Patent_Count, ...]` per paper (for histogramming)

Chat endpoint (`app_chat.py`):
- `POST /api/chat`
  - Body: `{ "message": "..." }`
  - Returns:
    - `assistant_text`
    - `intent`
    - `sql`
    - `data_preview` (first 20 rows)
    - `vega_lite_spec` (or `null` if invalid)
    - `issues` (validation issues)
    - `meta` (row/column info)
- `GET /health`
  - Returns `{ "ok": true }`

## Notes
- Graph endpoints currently filter to Computer Science and PSU affiliations via SQL in `services/*_network.py`.
- Timeline and patent histogram are currently hard-coded to PSU and a year range (2011–2021).
- The chat pipeline only supports intents listed in `services/tools.py::SUPPORTED_INTENTS`.

## Data Process
This backend uses two data paths: full TSVs for graph analytics and a smaller Parquet subset for chat/analytics.

Data resources and handling:
- TSV pipeline (graph endpoints): `services/data_loader.py` registers TSVs as DuckDB views at startup (`register_tables()` in `app.py`). Graph queries join these views and apply field + affiliation filters to build networks and aggregations.
- Parquet pipeline (chat endpoint): `services/data_loader_chat.py` registers Parquet files in `subset_data/` as views for a lighter, read-only analytics workload. `execute_sql()` enforces a single SELECT statement, blocks mutating SQL, and applies a default row limit when missing.

Multi-agent design and tools:
- The chat endpoint runs a LangGraph pipeline defined in `services/agent_graph.py`.
- Roles:
  - RouterAgent: selects an intent and optional year range/top-N based on the user request and available schema.
  - AnalystAgent: maps the intent to a deterministic SQL template (`build_sql()`).
  - VizAgent: confirms intent and drafts a concise response message.
  - CriticAgent: checks if the generated Vega-Lite spec is valid and reports issues.
- Tools (from `services/tools.py`):
  - `get_schema`: exposes available views/columns to the RouterAgent.
  - `run_sql`: executes safe, read-only SQL against the Parquet subset.
  - `build_vega_spec`: produces a Vega-Lite v5 spec from query rows.
  - `validate_vega_spec`: ensures required encodings and interactions (hover + selection) are present.

## Attribution

Designed and built by Yunkai Xu. OpenAI's GenAI tools assisted with code formatting,  documentation, and DuckDB/SQL query implementations.
