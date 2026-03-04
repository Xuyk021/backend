from flask import Flask
from flask_cors import CORS

from dotenv import load_dotenv
load_dotenv()

from routes.chat import chat_bp


def create_app():
    app = Flask(__name__)
    CORS(app)

    app.register_blueprint(chat_bp, url_prefix="/api")

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)