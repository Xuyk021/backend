from flask import Flask
from flask_cors import CORS

from routes.graph_routes import graph_bp
from services.data_loader import register_tables

app = Flask(__name__)
CORS(app)

register_tables()

app.register_blueprint(graph_bp, url_prefix="/api")


if __name__ == "__main__":
    app.run(debug=True)