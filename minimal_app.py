#!/usr/bin/env python3
"""
Ultra-minimal Reddit Brand Monitor for Railway
"""

import os
import sys
from flask import Flask, jsonify, render_template_string
from datetime import datetime

print(f"🚀 MINIMAL START - Python {sys.version}")
print(f"🔧 PORT: {os.getenv('PORT', 'Not set')}")
print(f"🔧 Working dir: {os.getcwd()}")

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string('''
    <!DOCTYPE html>
    <html>
    <head><title>Reddit Monitor - Starting Up</title></head>
    <body>
        <h1>Reddit Brand Monitor</h1>
        <p>🚀 System starting up...</p>
        <p>⏱️ {{ timestamp }}</p>
        <p>🔗 <a href="/health">Health Check</a></p>
    </body>
    </html>
    ''', timestamp=datetime.now().isoformat())

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "port": os.getenv('PORT'),
        "message": "Minimal Reddit monitor running"
    })

@app.route('/data')
def data():
    return jsonify([])  # Empty data for now

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5000))
    print(f"✅ Starting Flask on port {port}")
    try:
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        print(f"💥 Flask failed: {e}")
        raise