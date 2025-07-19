#!/usr/bin/env python3
"""
Minimal test app for Railway deployment debugging
"""

import os
from flask import Flask, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({
        "message": "Railway Test App Working!",
        "timestamp": datetime.utcnow().isoformat(),
        "port": os.getenv('PORT', 'Not set'),
        "python_version": str(os.sys.version)
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    })

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5000))
    print(f"🚀 Starting test app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)