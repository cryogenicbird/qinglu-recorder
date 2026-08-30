import os
from flask import Flask, send_from_directory

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, 'index.html')

if __name__ == "__main__":
    print("\n  录屏工作台 HTTP 服务已启动 -> http://127.0.0.1:5000")
    print("  请同时运行: python ws_server.py\n")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)