#!/usr/bin/env python3
"""
Minimal receiver for the Gravitino event publisher.

Listens on 127.0.0.1:9099, checks the bearer token, pretty-prints whatever arrives.
No dependencies. Ctrl-C to stop.

    python3 receiver.py
"""
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

TOKEN = "demo-token"
PORT = 9099
count = 0


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global count

        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {TOKEN}":
            print(f"  !! rejected: bad or missing Authorization ({auth!r})")
            self.send_response(401)
            self.end_headers()
            return

        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(202)
        self.end_headers()

        count += 1
        stamp = datetime.now().strftime("%H:%M:%S")
        try:
            event = json.loads(body)
            etype = event.get("type", "?")
            subject = event.get("subject", "?")
            actor = event.get("actor", "?")
            print(f"\n[{stamp}]  #{count}  {etype}")
            print(f"           {subject}   by {actor}")
            print("-" * 78)
            print(json.dumps(event, indent=2))
            print("-" * 78)
        except Exception:
            print(f"\n[{stamp}]  #{count}  unparseable body:")
            print(body.decode("utf-8", "replace"))
        sys.stdout.flush()

    def log_message(self, *args):
        pass  # quiet the default access log


if __name__ == "__main__":
    print(f"receiver listening on http://127.0.0.1:{PORT}/ingest")
    print(f"expecting: Authorization: Bearer {TOKEN}")
    print("waiting for events. Ctrl-C to stop.\n")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
