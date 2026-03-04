from flask import Blueprint, request, jsonify
from services.orchestrator import run_chat

chat_bp = Blueprint("chat", __name__)


@chat_bp.post("/chat")
def chat():
    payload = request.get_json(silent=True) or {}
    msg = (payload.get("message") or "").strip()
    if not msg:
        return jsonify({"error": "Missing message"}), 400

    try:
        out = run_chat(msg)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500