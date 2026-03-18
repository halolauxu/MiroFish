"""
Standalone API server for AStrategy frontend.
Bypasses original MiroFish dependencies (zep_cloud etc.)

Usage: python3 run_api.py
"""
import importlib.util
import sys
from pathlib import Path

from flask import Flask
from flask_cors import CORS

# Load the astrategy blueprint directly (bypass __init__.py imports)
spec = importlib.util.spec_from_file_location(
    "astrategy_api",
    Path(__file__).parent / "backend" / "app" / "api" / "astrategy.py",
)
mod = importlib.util.module_from_spec(spec)
sys.modules["astrategy_api"] = mod
spec.loader.exec_module(mod)

app = Flask(__name__)
CORS(app)
app.register_blueprint(mod.astrategy_bp)

if __name__ == "__main__":
    print("AStrategy API server starting on http://localhost:5001")
    print("Endpoints: /api/astrategy/...")
    app.run(host="0.0.0.0", port=5001, debug=False)
