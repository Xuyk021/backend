from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

from langgraph.graph import StateGraph, END

from services.tools import get_schema, run_sql, build_sql, build_vega_spec, validate_vega_spec, SUPPORTED_INTENTS


class GraphState(TypedDict, total=False):
    user_message: str
    schema: Dict[str, List[str]]

    intent: str
    year_min: Optional[int]
    year_max: Optional[int]
    top_n: int

    sql: str
    query_result: Dict[str, Any]

    spec: Dict[str, Any]
    spec_validation: Dict[str, Any]

    assistant_text: str
    error: str


class RouteDecision(BaseModel):
    intent: str = Field(description=f"One of: {SUPPORTED_INTENTS} or 'unsupported'")
    year_min: Optional[int] = None
    year_max: Optional[int] = None
    top_n: int = Field(default=20, ge=1, le=200)
    assistant_text: str = Field(default="")


class VizDecision(BaseModel):
    intent: str = Field(description=f"One of: {SUPPORTED_INTENTS}")
    assistant_text: str = Field(default="")


class CriticDecision(BaseModel):
    ok: bool
    assistant_text: str = Field(default="")


def _llm(model: str = "gpt-4o-mini") -> ChatOpenAI:
    return ChatOpenAI(model=model, temperature=0)


def build_multi_agent_graph():
    llm_router = _llm()
    llm_viz = _llm()
    llm_critic = _llm()

    def schema_tool_node(state: GraphState) -> GraphState:
        state["schema"] = get_schema.invoke({})
        return state

    def router_agent_node(state: GraphState) -> GraphState:
        msg = state["user_message"]
        schema = state.get("schema", {})

        sys = SystemMessage(
            content=(
                "You are RouterAgent. Decide which analysis intent to run based on user request "
                "and available schema. If not possible, choose intent='unsupported'."
            )
        )
        human = HumanMessage(
            content=(
                f"User message:\n{msg}\n\n"
                f"Available schema:\n{schema}\n\n"
                f"Supported intents:\n{SUPPORTED_INTENTS}\n\n"
                "Return a JSON decision."
            )
        )

        decision = llm_router.with_structured_output(RouteDecision).invoke([sys, human])

        state["intent"] = decision.intent
        state["year_min"] = decision.year_min
        state["year_max"] = decision.year_max
        state["top_n"] = decision.top_n
        state["assistant_text"] = decision.assistant_text or ""

        return state

    def analyst_agent_node(state: GraphState) -> GraphState:
        intent = state.get("intent", "unsupported")
        if intent == "unsupported":
            return state

        sql = build_sql(
            intent=intent,
            year_min=state.get("year_min"),
            year_max=state.get("year_max"),
            top_n=int(state.get("top_n") or 20),
        )
        print(sql)
        state["sql"] = sql
        return state

    def query_tool_node(state: GraphState) -> GraphState:
        if state.get("intent") == "unsupported":
            return state
        res = run_sql.invoke({"sql": state["sql"]})
        state["query_result"] = res
        return state

    def viz_agent_node(state: GraphState) -> GraphState:
        if state.get("intent") == "unsupported":
            if not state.get("assistant_text"):
                state["assistant_text"] = "Unsupported request for the current subset schema."
            return state

        msg = state["user_message"]
        intent = state["intent"]
        cols = state["query_result"]["columns"]

        sys = SystemMessage(
            content=(
                "You are VizAgent. Confirm the visualization intent and provide a short message. "
                "Do not write Vega-Lite JSON. A tool will build the spec."
            )
        )
        human = HumanMessage(
            content=(
                f"User message:\n{msg}\n\n"
                f"Intent:\n{intent}\n\n"
                f"Query result columns:\n{cols}\n\n"
                "Return JSON with the same intent and a concise assistant_text."
            )
        )
        decision = llm_viz.with_structured_output(VizDecision).invoke([sys, human])
        state["assistant_text"] = state.get("assistant_text") or decision.assistant_text or ""
        state["intent"] = decision.intent
        return state

    def spec_tool_node(state: GraphState) -> GraphState:
        if state.get("intent") == "unsupported":
            return state

        rows = state["query_result"]["rows"]
        spec = build_vega_spec.invoke({"intent": state["intent"], "rows": rows})
        state["spec"] = spec

        cols = state["query_result"]["columns"]
        state["spec_validation"] = validate_vega_spec.invoke({"spec": spec, "data_columns": cols})
        return state

    def critic_agent_node(state: GraphState) -> GraphState:
        if state.get("intent") == "unsupported":
            return state

        v = state.get("spec_validation", {})
        ok = bool(v.get("ok", False))
        issues = v.get("issues", [])

        sys = SystemMessage(
            content=(
                "You are CriticAgent. Decide whether the output is acceptable. "
                "If not ok, produce a short assistant message describing the issue."
            )
        )
        human = HumanMessage(
            content=(
                f"Spec validation ok={ok}\nIssues={issues}\n"
                "Return JSON with ok and assistant_text."
            )
        )
        decision = llm_critic.with_structured_output(CriticDecision).invoke([sys, human])

        if decision.assistant_text:
            state["assistant_text"] = decision.assistant_text if not state.get("assistant_text") else state["assistant_text"]

        if not ok:
            state["error"] = "Spec validation failed"
        return state

    def route_after_router(state: GraphState) -> str:
        if state.get("intent") == "unsupported":
            return "unsupported"
        return "supported"

    def route_after_critic(state: GraphState) -> str:
        v = state.get("spec_validation", {})
        if v.get("ok", False):
            return "done"
        return "done"

    g = StateGraph(GraphState)

    g.add_node("schema_tool", schema_tool_node)
    g.add_node("router_agent", router_agent_node)
    g.add_node("analyst_agent", analyst_agent_node)
    g.add_node("query_tool", query_tool_node)
    g.add_node("viz_agent", viz_agent_node)
    g.add_node("spec_tool", spec_tool_node)
    g.add_node("critic_agent", critic_agent_node)

    g.set_entry_point("schema_tool")
    g.add_edge("schema_tool", "router_agent")

    g.add_conditional_edges(
        "router_agent",
        route_after_router,
        {
            "supported": "analyst_agent",
            "unsupported": "viz_agent",
        },
    )

    g.add_edge("analyst_agent", "query_tool")
    g.add_edge("query_tool", "viz_agent")
    g.add_edge("viz_agent", "spec_tool")
    g.add_edge("spec_tool", "critic_agent")

    g.add_conditional_edges(
        "critic_agent",
        route_after_critic,
        {"done": END},
    )

    return g.compile()