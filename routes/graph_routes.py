from flask import Blueprint, request, jsonify
from services.citation_network import get_citation_network
from services.collaboration_network import get_author_collab
from services.dashboard import get_cs_timeline, get_patent_histogram


graph_bp = Blueprint("graph", __name__)


@graph_bp.route("/citation-network")
def citation():

    university = request.args.get("university")

    data = get_citation_network(university)

    return jsonify(data)


@graph_bp.route("/author-network")
def collaboration():

    university = request.args.get("university")

    data = get_author_collab(university)

    return jsonify(data)

@graph_bp.route("/timeline")
def timeline():
    return jsonify(get_cs_timeline())

@graph_bp.route("/patent-histogram")
def histogram():
    year = request.args.get("year")
    return jsonify(get_patent_histogram(year))