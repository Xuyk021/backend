from __future__ import annotations

from typing import Any, Dict

from services.agent_graph import build_multi_agent_graph

_GRAPH = build_multi_agent_graph()


def run_chat(user_message: str) -> Dict[str, Any]:
    state = {"user_message": user_message}
    out = _GRAPH.invoke(state)

    intent = out.get("intent", "unsupported")
    assistant_text = out.get("assistant_text") or ""

    if intent == "unsupported":
        print(f"Unsupported intent detected. Assistant text: {assistant_text}")
        return {
            "assistant_text": assistant_text or "Unsupported request for the current subset schema.",
            "intent": "unsupported",
            "sql": None,
            "data_preview": [],
            "vega_lite_spec": None,
            "issues": [],
            "schema": out.get("schema", {}),
        }

    qr = out.get("query_result", {})
    rows = qr.get("rows", [])
    cols = qr.get("columns", [])
    spec = out.get("spec", None)
    v = out.get("spec_validation", {}) or {}
    issues = v.get("issues", [])

    return {
        "assistant_text": assistant_text or f"Showing results for: {intent}",
        "intent": intent,
        "sql": qr.get("sql"),
        "data_preview": rows[:20],
        "vega_lite_spec": spec if v.get("ok", False) else None,
        "issues": issues,
        "meta": {"n_rows": qr.get("n_rows", 0), "columns": cols},
    }