from flask import Flask, jsonify
import subprocess
import os
from pathlib import Path

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Rainmaker Data Sync API", "endpoints": ["/sync/<client>", "/update/<client>"]})

@app.route('/sync/<client>')
def sync_client(client):
    try:
        # Run the sync script for the specified client
        result = subprocess.run(
            ['python', 'sync.py', '--client', client],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
            timeout=43200  # 12 hours for full sync
        )
        return jsonify({
            "client": client,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Script timed out after 12 hours"}), 408
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update/<client>')
def update_client(client):
    try:
        # Run the update script for the specified client
        result = subprocess.run(
            ['python', 'update.py', '--client', client],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
            timeout=7200  # 2 hours for update
        )
        return jsonify({
            "client": client,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Script timed out after 2 hours"}), 408
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)