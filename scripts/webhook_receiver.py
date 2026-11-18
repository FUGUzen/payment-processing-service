import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        secret = os.getenv("WEBHOOK_SIGNING_SECRET")
        if secret:
            timestamp = self.headers.get("X-Webhook-Timestamp", "")
            signature = self.headers.get("X-Webhook-Signature", "")
            expected = (
                "v1="
                + hmac.new(
                    secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
                ).hexdigest()
            )
            try:
                fresh = abs(time.time() - int(timestamp)) <= 300
            except ValueError:
                fresh = False
            if not fresh or not hmac.compare_digest(signature, expected):
                self.send_response(403)
                self.end_headers()
                return
        print(
            json.dumps({"event_id": self.headers.get("X-Event-ID"), "body": json.loads(body)}),
            flush=True,
        )
        self.send_response(int(os.getenv("WEBHOOK_STATUS", "200")))
        self.end_headers()


HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
