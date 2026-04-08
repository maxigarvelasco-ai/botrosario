import os
from flask import Flask, jsonify, request

import telegramBot

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def root():
    result = telegramBot.telegram_webhook(request)
    if not isinstance(result, tuple):
        return result

    body = result[0] if len(result) > 0 else {}
    status = result[1] if len(result) > 1 else 200
    headers = result[2] if len(result) > 2 and isinstance(result[2], dict) else {}

    if isinstance(body, (dict, list)):
        resp = jsonify(body)
        resp.status_code = int(status)
    else:
        mimetype = headers.get("Content-Type", "application/json")
        resp = app.response_class(response=str(body), status=int(status), mimetype=mimetype)

    for key, value in headers.items():
        resp.headers[key] = value
    return resp


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
