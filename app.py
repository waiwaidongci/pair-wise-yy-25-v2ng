from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from database import CorpusDB, DomainError

BASE = Path(__file__).resolve().parent
DB_PATH = os.environ.get("CORPUS_DB", str(BASE / "corpus.db"))


class Handler(BaseHTTPRequestHandler):
    db = CorpusDB(DB_PATH)

    def log_message(self, fmt, *args):
        return

    def _json(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        try:
            value = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise DomainError("请求体必须是合法 JSON") from exc
        if not isinstance(value, dict):
            raise DomainError("请求体必须是 JSON 对象")
        return value

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            parts = [p for p in parsed.path.split("/") if p]
            if parsed.path in ("/", "/index.html"):
                data = (BASE / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if parsed.path == "/api/state":
                return self._json(200, self.db.snapshot())
            if len(parts) == 3 and parts[:2] == ["api", "items"]:
                user_id = int(parse_qs(parsed.query).get("user_id", [0])[0])
                return self._json(200, self.db.get_item_for_user(int(parts[2]), user_id))
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "disagreements":
                return self._json(200, {"disagreements": self.db.disagreements(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "consistency":
                return self._json(200, self.db.consistency(int(parts[2])))
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "gold":
                return self._json(200, self.db.export_gold(int(parts[2])))
            self._json(404, {"ok": False, "error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._json(400, {"ok": False, "error": str(exc)})

    def do_POST(self):
        try:
            body, parts = self._body(), [p for p in urlparse(self.path).path.split("/") if p]
            path = "/" + "/".join(parts)
            if path == "/api/users":
                return self._json(201, {"ok": True, "id": self.db.add_user(str(body.get("name", "")), str(body.get("role", "annotator")))})
            if path == "/api/guidelines":
                return self._json(201, {"ok": True, "id": self.db.add_guideline(str(body.get("version", "")), str(body.get("rules", "")))})
            if path == "/api/batches":
                return self._json(201, {"ok": True, "id": self.db.create_batch(str(body.get("name", "")), int(body.get("guideline_id", 0)))})
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "items":
                return self._json(201, {"ok": True, "id": self.db.add_item(int(parts[2]), int(body.get("ordinal", 0)), str(body.get("text", "")))})
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "assign":
                return self._json(201, {"ok": True, "id": self.db.assign(int(body.get("item_id", 0)), int(body.get("annotator_id", 0)))})
            if path == "/api/annotations":
                return self._json(201, {"ok": True, "id": self.db.submit_annotation(int(body.get("item_id", 0)), int(body.get("annotator_id", 0)), str(body.get("label", "")), str(body.get("comment", "")))})
            if path == "/api/adjudications":
                return self._json(201, {"ok": True, "id": self.db.adjudicate(int(body.get("item_id", 0)), str(body.get("final_label", "")), str(body.get("reason", "")), int(body.get("arbitrator_id", 0)))})
            if path == "/api/discussions":
                return self._json(201, {"ok": True, "id": self.db.add_discussion(int(body.get("item_id", 0)), int(body.get("author_id", 0)), str(body.get("body", "")), bool(body.get("contains_answer", False)))})
            if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "freeze":
                return self._json(200, {"ok": True, **self.db.freeze_batch(int(parts[2]), int(body.get("manager_id", 0)))})
            self._json(404, {"ok": False, "error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._json(400, {"ok": False, "error": str(exc)})


def main():
    CorpusDB(DB_PATH).seed_demo()
    port = int(os.environ.get("PORT", "8112"))
    print(f"Corpus adjudication service: http://127.0.0.1:{port}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
