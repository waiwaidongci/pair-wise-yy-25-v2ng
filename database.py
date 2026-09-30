from __future__ import annotations

import json
import sqlite3
import threading
from collections import Counter
from contextlib import contextmanager
from datetime import datetime


class DomainError(ValueError):
    """Business rule violation."""


class ConflictError(DomainError):
    """版本过期或批次已冻结等冲突，映射为 HTTP 409。"""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


class CorpusDB:
    """A small multi-annotator corpus governance service.

    批次带有单调递增的 ``version``：每次标注/仲裁推进一个版本并写入
    ``batch_changes`` 变化表；冻结时客户端必须带上读取到的版本号，版本过期
    返回 :class:`ConflictError`（HTTP 409）并列出期间变化。冻结在单个写事务
    内把条目、标注、仲裁、指标写入不可变快照；``request_id`` 相同的重试直接
    返回首次结果。
    """

    def __init__(self, path: str = "corpus.db") -> None:
        self.path = path
        self._shared_memory = path == ":memory:"
        self._tls = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        # :memory: 库无法跨连接共享，只能用单连接 + 锁串行化写入
        self._memory_lock = threading.RLock()
        self._shared_conn = self._connect() if self._shared_memory else None
        # 同一 request_id 的并发请求：后到者等首笔落库后复用其结果
        self._inflight: dict[str, threading.Event] = {}
        self._inflight_lock = threading.Lock()
        self._schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        if self.path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self._shared_memory:
            return self._shared_conn
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = self._connect()
            self._tls.conn = conn
        return conn

    def close(self) -> None:
        with self._conns_lock:
            for conn in self._all_conns:
                conn.close()
            self._all_conns.clear()
        self._tls = threading.local()

    @contextmanager
    def transaction(self):
        if self._shared_memory:
            self._memory_lock.acquire()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        finally:
            if self._shared_memory:
                self._memory_lock.release()

    def _schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              role TEXT NOT NULL CHECK(role IN ('annotator','arbitrator','manager'))
            );
            CREATE TABLE IF NOT EXISTS guidelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              version TEXT NOT NULL UNIQUE,
              rules TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
            );
            CREATE TABLE IF NOT EXISTS batches (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              guideline_id INTEGER NOT NULL REFERENCES guidelines(id),
              status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','annotating','frozen')),
              version INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
              ordinal INTEGER NOT NULL,
              text TEXT NOT NULL,
              UNIQUE(batch_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS assignments (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
              item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
              annotator_id INTEGER NOT NULL REFERENCES users(id),
              status TEXT NOT NULL DEFAULT 'assigned' CHECK(status IN ('assigned','submitted')),
              UNIQUE(item_id, annotator_id)
            );
            CREATE TABLE IF NOT EXISTS annotations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
              annotator_id INTEGER NOT NULL REFERENCES users(id),
              guideline_id INTEGER NOT NULL REFERENCES guidelines(id),
              label TEXT NOT NULL,
              comment TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              UNIQUE(item_id, annotator_id, guideline_id)
            );
            CREATE TABLE IF NOT EXISTS adjudications (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              item_id INTEGER NOT NULL UNIQUE REFERENCES items(id) ON DELETE CASCADE,
              guideline_id INTEGER NOT NULL REFERENCES guidelines(id),
              final_label TEXT NOT NULL,
              reason TEXT NOT NULL,
              arbitrator_id INTEGER NOT NULL REFERENCES users(id),
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS discussions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
              author_id INTEGER NOT NULL REFERENCES users(id),
              body TEXT NOT NULL,
              contains_answer INTEGER NOT NULL DEFAULT 0 CHECK(contains_answer IN (0,1)),
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gold_records (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
              item_id INTEGER NOT NULL UNIQUE REFERENCES items(id),
              guideline_id INTEGER NOT NULL REFERENCES guidelines(id),
              label TEXT NOT NULL,
              source TEXT NOT NULL CHECK(source IN ('consensus','adjudication')),
              adjudication_id INTEGER REFERENCES adjudications(id),
              frozen_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batch_freezes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL UNIQUE REFERENCES batches(id),
              metrics_json TEXT NOT NULL,
              frozen_by INTEGER NOT NULL REFERENCES users(id),
              frozen_at TEXT NOT NULL,
              request_id TEXT,
              version INTEGER,
              guideline_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS batch_changes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
              version INTEGER NOT NULL,
              kind TEXT NOT NULL,
              item_id INTEGER REFERENCES items(id) ON DELETE SET NULL,
              actor_id INTEGER REFERENCES users(id),
              summary TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshot_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              freeze_id INTEGER NOT NULL REFERENCES batch_freezes(id) ON DELETE CASCADE,
              item_id INTEGER NOT NULL,
              ordinal INTEGER NOT NULL,
              text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshot_annotations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              freeze_id INTEGER NOT NULL REFERENCES batch_freezes(id) ON DELETE CASCADE,
              item_id INTEGER NOT NULL,
              annotation_id INTEGER,
              annotator_id INTEGER NOT NULL,
              label TEXT NOT NULL,
              comment TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshot_adjudications (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              freeze_id INTEGER NOT NULL REFERENCES batch_freezes(id) ON DELETE CASCADE,
              item_id INTEGER NOT NULL,
              adjudication_id INTEGER,
              arbitrator_id INTEGER NOT NULL,
              final_label TEXT NOT NULL,
              reason TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS freeze_requests (
              request_id TEXT PRIMARY KEY,
              batch_id INTEGER NOT NULL REFERENCES batches(id),
              response_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_batch_changes_version
              ON batch_changes(batch_id, version);
            """
        )
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        """给早于版本化特性的旧库补齐列；必要时把 batch_freezes 重建为带 id 的形态。"""
        batch_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(batches)")}
        if "version" not in batch_cols:
            self.conn.execute("ALTER TABLE batches ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
        freeze_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(batch_freezes)")}
        if freeze_cols and "id" not in freeze_cols:
            # 旧形态以 batch_id 作主键，快照表无法以其作外键父键，重建为带 id 的表
            self.conn.execute("PRAGMA foreign_keys=OFF")
            self.conn.execute("ALTER TABLE batch_freezes RENAME TO batch_freezes_legacy")
            self.conn.execute(
                "CREATE TABLE batch_freezes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "batch_id INTEGER NOT NULL UNIQUE REFERENCES batches(id),"
                "metrics_json TEXT NOT NULL,"
                "frozen_by INTEGER NOT NULL REFERENCES users(id),"
                "frozen_at TEXT NOT NULL,"
                "request_id TEXT,"
                "version INTEGER,"
                "guideline_id INTEGER)"
            )
            self.conn.execute(
                "INSERT INTO batch_freezes(batch_id,metrics_json,frozen_by,frozen_at,request_id,version,guideline_id)"
                " SELECT batch_id,metrics_json,frozen_by,frozen_at,NULL,NULL,NULL FROM batch_freezes_legacy"
            )
            self.conn.execute("DROP TABLE batch_freezes_legacy")
            # 旧冻结批次没有快照表数据，按冻结时的金标准与现有标注/仲裁回填，保证导出完整
            legacy = self.conn.execute("SELECT id,batch_id FROM batch_freezes").fetchall()
            for fr in legacy:
                items = self.conn.execute(
                    "SELECT i.id,i.ordinal,i.text FROM items i JOIN gold_records g ON g.item_id=i.id "
                    "WHERE g.batch_id=? ORDER BY i.ordinal", (fr["batch_id"],)
                ).fetchall()
                for it in items:
                    self.conn.execute(
                        "INSERT INTO snapshot_items(freeze_id,item_id,ordinal,text) VALUES(?,?,?,?)",
                        (fr["id"], it["id"], it["ordinal"], it["text"]),
                    )
                    for ann in self.conn.execute(
                        "SELECT id,annotator_id,label,comment,created_at FROM annotations WHERE item_id=?",
                        (it["id"],),
                    ).fetchall():
                        self.conn.execute(
                            "INSERT INTO snapshot_annotations"
                            "(freeze_id,item_id,annotation_id,annotator_id,label,comment,created_at)"
                            " VALUES(?,?,?,?,?,?,?)",
                            (fr["id"], it["id"], ann["id"], ann["annotator_id"],
                             ann["label"], ann["comment"], ann["created_at"]),
                        )
                    adj = self.conn.execute(
                        "SELECT id,arbitrator_id,final_label,reason,created_at "
                        "FROM adjudications WHERE item_id=?", (it["id"],)
                    ).fetchone()
                    if adj:
                        self.conn.execute(
                            "INSERT INTO snapshot_adjudications"
                            "(freeze_id,item_id,adjudication_id,arbitrator_id,final_label,reason,created_at)"
                            " VALUES(?,?,?,?,?,?,?)",
                            (fr["id"], it["id"], adj["id"], adj["arbitrator_id"],
                             adj["final_label"], adj["reason"], adj["created_at"]),
                        )
            self.conn.execute("PRAGMA foreign_keys=ON")
        else:
            for name, decl in (
                ("request_id", "TEXT"),
                ("version", "INTEGER"),
                ("guideline_id", "INTEGER"),
            ):
                if name not in freeze_cols:
                    self.conn.execute(f"ALTER TABLE batch_freezes ADD COLUMN {name} {decl}")

    # ---- 版本与变化 ------------------------------------------------------

    def _advance_version(self, batch_id: int, kind: str, item_id: int | None,
                         actor_id: int | None, summary: str) -> int:
        """在当前事务内推进批次版本并追加一条变化记录，返回新版本号。"""
        self.conn.execute("UPDATE batches SET version=version+1 WHERE id=?", (batch_id,))
        version = self.conn.execute(
            "SELECT version FROM batches WHERE id=?", (batch_id,)
        ).fetchone()["version"]
        self.conn.execute(
            "INSERT INTO batch_changes(batch_id,version,kind,item_id,actor_id,summary,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (batch_id, version, kind, item_id, actor_id, summary, datetime.now().isoformat()),
        )
        return version

    def _changes_since(self, batch_id: int, version: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT version,kind,item_id,actor_id,summary,created_at FROM batch_changes "
            "WHERE batch_id=? AND version>? ORDER BY version,id",
            (batch_id, version),
        ).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> None:
        if self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]:
            return
        a1 = self.add_user("标注员甲", "annotator")
        a2 = self.add_user("标注员乙", "annotator")
        arb = self.add_user("仲裁员", "arbitrator")
        guideline = self.add_guideline("v1", "标签仅可为 正向/负向/中性；先独立标注，不得查看他人答案。")
        batch = self.create_batch("情感标注示例", guideline)
        item1 = self.add_item(batch, 1, "这个更新让工作流畅了很多。")
        item2 = self.add_item(batch, 2, "功能没有变化，但也没有明显问题。")
        self.assign(item1, a1)
        self.assign(item1, a2)
        self.assign(item2, a1)
        self.assign(item2, a2)
        self.submit_annotation(item1, a1, "正向", "整体表达积极")
        self.submit_annotation(item1, a2, "中性", "描述较克制")
        self.submit_annotation(item2, a1, "中性")
        self.submit_annotation(item2, a2, "中性")

    def add_user(self, name: str, role: str) -> int:
        if not name.strip() or role not in {"annotator", "arbitrator", "manager"}:
            raise DomainError("用户名或角色无效")
        with self.transaction():
            try:
                cur = self.conn.execute("INSERT INTO users(name,role) VALUES(?,?)", (name.strip(), role))
            except sqlite3.IntegrityError as exc:
                raise DomainError("用户名已存在") from exc
        return int(cur.lastrowid)

    def add_guideline(self, version: str, rules: str) -> int:
        if not version.strip() or not rules.strip():
            raise DomainError("指南版本和规则不能为空")
        with self.transaction():
            cur = self.conn.execute("INSERT INTO guidelines(version,rules) VALUES(?,?)", (version.strip(), rules.strip()))
        return int(cur.lastrowid)

    def create_batch(self, name: str, guideline_id: int) -> int:
        if not name.strip() or not self.conn.execute("SELECT 1 FROM guidelines WHERE id=? AND active=1", (guideline_id,)).fetchone():
            raise DomainError("批次名称或指南无效")
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO batches(name,guideline_id,created_at) VALUES(?,?,?)",
                (name.strip(), guideline_id, datetime.now().isoformat()),
            )
        return int(cur.lastrowid)

    def add_item(self, batch_id: int, ordinal: int, text: str) -> int:
        if ordinal <= 0 or not text.strip():
            raise DomainError("序号必须大于0且文本不能为空")
        with self.transaction():
            batch = self.conn.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not batch or batch["status"] == "frozen":
                raise ConflictError("批次不存在或已经冻结，不能新增条目")
            try:
                cur = self.conn.execute("INSERT INTO items(batch_id,ordinal,text) VALUES(?,?,?)", (batch_id, ordinal, text.strip()))
            except sqlite3.IntegrityError as exc:
                raise DomainError("该批次序号已存在") from exc
        return int(cur.lastrowid)

    def assign(self, item_id: int, annotator_id: int) -> int:
        item = self.conn.execute("SELECT batch_id FROM items WHERE id=?", (item_id,)).fetchone()
        user = self.conn.execute("SELECT role FROM users WHERE id=?", (annotator_id,)).fetchone()
        if not item or not user or user["role"] != "annotator":
            raise DomainError("条目不存在或用户不是标注员")
        with self.transaction():
            batch = self.conn.execute("SELECT status FROM batches WHERE id=?", (item["batch_id"],)).fetchone()
            if not batch or batch["status"] == "frozen":
                raise ConflictError("批次已冻结，不能再领取标注任务")
            self.conn.execute("UPDATE batches SET status='annotating' WHERE id=? AND status='draft'", (item["batch_id"],))
            try:
                cur = self.conn.execute(
                    "INSERT INTO assignments(batch_id,item_id,annotator_id) VALUES(?,?,?)",
                    (item["batch_id"], item_id, annotator_id),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("同一标注员不能重复领取同一条目") from exc
        return int(cur.lastrowid)

    def submit_annotation(self, item_id: int, annotator_id: int, label: str, comment: str = "") -> int:
        if not label.strip():
            raise DomainError("标签不能为空")
        with self.transaction():
            # 冻结判断必须在写事务内，否则与并发冻结之间存在 TOCTOU 窗口
            item = self.conn.execute(
                "SELECT i.id,i.batch_id,b.guideline_id,b.status,b.version "
                "FROM items i JOIN batches b ON b.id=i.batch_id WHERE i.id=?", (item_id,)
            ).fetchone()
            assignment = self.conn.execute(
                "SELECT id FROM assignments WHERE item_id=? AND annotator_id=?", (item_id, annotator_id)
            ).fetchone()
            if not item or not assignment:
                raise DomainError("只能提交已分配条目的标注")
            if item["status"] == "frozen":
                raise ConflictError(
                    f"批次已冻结（版本 {item['version']}），标注落在冻结之后",
                    {"current_version": item["version"]},
                )
            existed = self.conn.execute(
                "SELECT 1 FROM annotations WHERE item_id=? AND annotator_id=? AND guideline_id=?",
                (item_id, annotator_id, item["guideline_id"]),
            ).fetchone()
            invalidated = self.conn.execute(
                "SELECT 1 FROM adjudications WHERE item_id=?", (item_id,)
            ).fetchone()
            self.conn.execute("DELETE FROM adjudications WHERE item_id=?", (item_id,))
            try:
                cur = self.conn.execute(
                    "INSERT INTO annotations(item_id,annotator_id,guideline_id,label,comment,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, annotator_id, item["guideline_id"], label.strip(), comment.strip(), datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError:
                self.conn.execute(
                    "UPDATE annotations SET label=?,comment=?,created_at=? WHERE item_id=? AND annotator_id=? AND guideline_id=?",
                    (label.strip(), comment.strip(), datetime.now().isoformat(), item_id, annotator_id, item["guideline_id"]),
                )
                annotation_id = self.conn.execute(
                    "SELECT id FROM annotations WHERE item_id=? AND annotator_id=? AND guideline_id=?",
                    (item_id, annotator_id, item["guideline_id"]),
                ).fetchone()["id"]
            else:
                annotation_id = int(cur.lastrowid)
            self.conn.execute("UPDATE assignments SET status='submitted' WHERE id=?", (assignment["id"],))
            action = "修改标注为" if existed else "提交标注"
            summary = f"标注员#{annotator_id} {action}「{label.strip()}」"
            if invalidated:
                summary += "；原仲裁随之作废"
            self._advance_version(
                item["batch_id"],
                "annotation_updated" if existed else "annotation_submitted",
                item_id, annotator_id, summary,
            )
        return int(annotation_id)

    def add_discussion(self, item_id: int, author_id: int, body: str, contains_answer: bool = False) -> int:
        if not body.strip():
            raise DomainError("讨论内容不能为空")
        if not self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone():
            raise DomainError("条目不存在")
        if not self.conn.execute("SELECT 1 FROM users WHERE id=?", (author_id,)).fetchone():
            raise DomainError("用户不存在")
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO discussions(item_id,author_id,body,contains_answer,created_at) VALUES(?,?,?,?,?)",
                (item_id, author_id, body.strip(), int(contains_answer), datetime.now().isoformat()),
            )
        return int(cur.lastrowid)

    def get_item_for_user(self, item_id: int, user_id: int) -> dict:
        item = self.conn.execute(
            "SELECT i.id,i.batch_id,i.ordinal,i.text,g.version AS guideline_version,g.rules "
            "FROM items i JOIN batches b ON b.id=i.batch_id JOIN guidelines g ON g.id=b.guideline_id WHERE i.id=?",
            (item_id,),
        ).fetchone()
        if not item:
            raise DomainError("条目不存在")
        own = self.conn.execute(
            "SELECT id,label,comment,created_at FROM annotations WHERE item_id=? AND annotator_id=?", (item_id, user_id)
        ).fetchone()
        revealed = own is not None
        discussions = []
        for row in self.conn.execute(
            "SELECT d.*,u.name FROM discussions d JOIN users u ON u.id=d.author_id WHERE d.item_id=? ORDER BY d.id", (item_id,)
        ).fetchall():
            if row["contains_answer"] and not revealed:
                discussions.append({"id": row["id"], "author": row["name"], "body": "提交自己的标注后才能查看此讨论", "hidden": True})
            else:
                discussions.append(dict(row))
        payload = dict(item)
        payload["own_annotation"] = dict(own) if own else None
        payload["discussions"] = discussions
        return payload

    def get_batch(self, batch_id: int) -> dict:
        batch = self.conn.execute(
            "SELECT id,name,status,version,guideline_id FROM batches WHERE id=?", (batch_id,)
        ).fetchone()
        if not batch:
            raise DomainError("批次不存在")
        payload = dict(batch)
        freeze = self.conn.execute(
            "SELECT frozen_at,request_id FROM batch_freezes WHERE batch_id=?", (batch_id,)
        ).fetchone()
        payload["frozen_at"] = freeze["frozen_at"] if freeze else None
        payload["freeze_request_id"] = freeze["request_id"] if freeze else None
        return payload

    def disagreements(self, batch_id: int) -> list[dict]:
        batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("批次不存在")
        result = []
        for item in self.conn.execute("SELECT * FROM items WHERE batch_id=? ORDER BY ordinal", (batch_id,)).fetchall():
            rows = self.conn.execute(
                "SELECT a.*,u.name FROM annotations a JOIN users u ON u.id=a.annotator_id "
                "WHERE a.item_id=? AND a.guideline_id=? ORDER BY a.id",
                (item["id"], batch["guideline_id"]),
            ).fetchall()
            labels = {row["label"] for row in rows}
            adj = self.conn.execute("SELECT * FROM adjudications WHERE item_id=?", (item["id"],)).fetchone()
            if len(rows) >= 2 and len(labels) > 1 and not adj:
                result.append({
                    "item_id": item["id"], "ordinal": item["ordinal"], "text": item["text"],
                    "labels": [dict(row) for row in rows],
                })
        return result

    def adjudicate(self, item_id: int, final_label: str, reason: str, arbitrator_id: int) -> int:
        if not final_label.strip() or len(reason.strip()) < 5:
            raise DomainError("最终标签必填，仲裁理由至少5个字符")
        with self.transaction():
            user = self.conn.execute("SELECT role FROM users WHERE id=?", (arbitrator_id,)).fetchone()
            item = self.conn.execute(
                "SELECT i.id,i.batch_id,b.guideline_id,b.status,b.version "
                "FROM items i JOIN batches b ON b.id=i.batch_id WHERE i.id=?", (item_id,)
            ).fetchone()
            if not item or not user or user["role"] != "arbitrator":
                raise DomainError("条目或仲裁员无效")
            if item["status"] == "frozen":
                raise ConflictError(
                    f"批次已冻结（版本 {item['version']}），仲裁落在冻结之后",
                    {"current_version": item["version"]},
                )
            rows = self.conn.execute("SELECT label FROM annotations WHERE item_id=?", (item_id,)).fetchall()
            if len(rows) < 2:
                raise DomainError("至少需要两份标注才能仲裁")
            existed = self.conn.execute("SELECT 1 FROM adjudications WHERE item_id=?", (item_id,)).fetchone()
            try:
                cur = self.conn.execute(
                    "INSERT INTO adjudications(item_id,guideline_id,final_label,reason,arbitrator_id,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, item["guideline_id"], final_label.strip(), reason.strip(), arbitrator_id, datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError:
                self.conn.execute(
                    "UPDATE adjudications SET final_label=?,reason=?,arbitrator_id=?,created_at=? WHERE item_id=?",
                    (final_label.strip(), reason.strip(), arbitrator_id, datetime.now().isoformat(), item_id),
                )
                adjudication_id = self.conn.execute("SELECT id FROM adjudications WHERE item_id=?", (item_id,)).fetchone()["id"]
            else:
                adjudication_id = int(cur.lastrowid)
            action = "修改仲裁为" if existed else "提交仲裁"
            self._advance_version(
                item["batch_id"],
                "adjudication_updated" if existed else "adjudication_submitted",
                item_id, arbitrator_id,
                f"仲裁员#{arbitrator_id} {action}「{final_label.strip()}」",
            )
        return int(adjudication_id)

    def consistency(self, batch_id: int) -> dict:
        batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("批次不存在")
        items = self.conn.execute("SELECT id,ordinal FROM items WHERE batch_id=? ORDER BY ordinal", (batch_id,)).fetchall()
        per_item, agreement_pairs, total_pairs = [], 0, 0
        label_totals: Counter[str] = Counter()
        all_annotation_count = 0
        for item in items:
            labels = [r["label"] for r in self.conn.execute(
                "SELECT label FROM annotations WHERE item_id=? AND guideline_id=?", (item["id"], batch["guideline_id"])
            ).fetchall()]
            if len(labels) < 2:
                per_item.append({"item_id": item["id"], "ordinal": item["ordinal"], "agreement": None, "annotations": len(labels)})
                continue
            pairs = total = 0
            for i in range(len(labels)):
                for j in range(i + 1, len(labels)):
                    total += 1
                    pairs += labels[i] == labels[j]
            agreement = pairs / total
            agreement_pairs += pairs
            total_pairs += total
            all_annotation_count += len(labels)
            label_totals.update(labels)
            per_item.append({"item_id": item["id"], "ordinal": item["ordinal"], "agreement": round(agreement, 4), "annotations": len(labels)})
        pairwise = agreement_pairs / total_pairs if total_pairs else None
        expected = sum((count / all_annotation_count) ** 2 for count in label_totals.values()) if all_annotation_count else None
        kappa = None
        if pairwise is not None and expected is not None and expected < 1:
            kappa = (pairwise - expected) / (1 - expected)
        return {
            "batch_id": batch_id,
            "version": batch["version"],
            "items_with_multiple_annotations": total_pairs and sum(1 for row in per_item if row["agreement"] is not None),
            "pairwise_agreement": round(pairwise, 4) if pairwise is not None else None,
            "fleiss_kappa": round(kappa, 4) if kappa is not None else None,
            "items": per_item,
        }

    def freeze_batch(self, batch_id: int, manager_id: int,
                     expected_version: int | None = None, request_id: str | None = None) -> dict:
        if request_id is not None:
            request_id = str(request_id).strip()
            if not request_id:
                raise DomainError("请求号不能为空")
        if expected_version is not None:
            expected_version = int(expected_version)

        # 同请求号并发：只让首个请求真正执行，其余等它落库后返回首次结果
        if request_id is not None:
            with self._inflight_lock:
                event = self._inflight.get(request_id)
                if event is None:
                    event = threading.Event()
                    self._inflight[request_id] = event
                    leader = True
                else:
                    leader = False
            if not leader:
                event.wait(30)
                with self.transaction():
                    cached = self.conn.execute(
                        "SELECT batch_id,response_json FROM freeze_requests WHERE request_id=?",
                        (request_id,),
                    ).fetchone()
                if cached:
                    if cached["batch_id"] != batch_id:
                        raise DomainError("请求号已用于其他批次")
                    return json.loads(cached["response_json"])
                # 首笔未成功（校验失败/回滚），放行让本请求尝试
        else:
            leader = False

        try:
            return self._freeze_batch(batch_id, manager_id, expected_version, request_id)
        finally:
            if leader:
                with self._inflight_lock:
                    self._inflight.pop(request_id, None)
                event.set()

    def _freeze_batch(self, batch_id: int, manager_id: int,
                      expected_version: int | None, request_id: str | None) -> dict:
        with self.transaction():
            # 幂等：同一请求号直接返回首次结果（含调用方拿着旧版本重试的情况）
            if request_id is not None:
                cached = self.conn.execute(
                    "SELECT batch_id,response_json FROM freeze_requests WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                if cached:
                    if cached["batch_id"] != batch_id:
                        raise DomainError("请求号已用于其他批次")
                    return json.loads(cached["response_json"])

            manager = self.conn.execute("SELECT role FROM users WHERE id=?", (manager_id,)).fetchone()
            batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not batch or not manager or manager["role"] != "manager":
                raise DomainError("批次或管理员无效")
            if batch["status"] == "frozen":
                raise ConflictError(
                    f"批次已冻结（版本 {batch['version']}），金标准已生成",
                    {"current_version": batch["version"]},
                )
            if expected_version is not None and expected_version != batch["version"]:
                changes = self._changes_since(batch_id, expected_version)
                raise ConflictError(
                    f"批次版本已过期：读取时版本 {expected_version}，当前版本 {batch['version']}",
                    {
                        "expected_version": expected_version,
                        "current_version": batch["version"],
                        "changes": changes,
                    },
                )

            items = self.conn.execute(
                "SELECT id,ordinal,text FROM items WHERE batch_id=? ORDER BY ordinal", (batch_id,)
            ).fetchall()
            if not items:
                raise DomainError("空批次不能冻结")
            unresolved = self.disagreements(batch_id)
            if unresolved:
                raise DomainError(f"仍有 {len(unresolved)} 条分歧未仲裁")
            missing = []
            for item in items:
                count = self.conn.execute(
                    "SELECT COUNT(*) FROM annotations WHERE item_id=? AND guideline_id=?",
                    (item["id"], batch["guideline_id"]),
                ).fetchone()[0]
                if count < 2:
                    missing.append(item["id"])
            if missing:
                raise DomainError(f"条目缺少至少两份标注: {missing}")

            # 校验通过后才计算指标；指标连同快照在同一事务内落库
            metrics = self.consistency(batch_id)
            frozen_at = datetime.now().isoformat()
            frozen_version = batch["version"]

            cur = self.conn.execute(
                "INSERT INTO batch_freezes"
                "(batch_id,metrics_json,frozen_by,frozen_at,request_id,version,guideline_id)"
                " VALUES(?,?,?,?,?,?,?)",
                (batch_id, json.dumps(metrics, ensure_ascii=False), manager_id,
                 frozen_at, request_id, frozen_version, batch["guideline_id"]),
            )
            freeze_id = int(cur.lastrowid)

            for item in items:
                ann_rows = self.conn.execute(
                    "SELECT id,annotator_id,label,comment,created_at FROM annotations "
                    "WHERE item_id=? AND guideline_id=? ORDER BY id",
                    (item["id"], batch["guideline_id"]),
                ).fetchall()
                for ann in ann_rows:
                    self.conn.execute(
                        "INSERT INTO snapshot_annotations"
                        "(freeze_id,item_id,annotation_id,annotator_id,label,comment,created_at)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (freeze_id, item["id"], ann["id"], ann["annotator_id"],
                         ann["label"], ann["comment"], ann["created_at"]),
                    )
                adj = self.conn.execute(
                    "SELECT id,arbitrator_id,final_label,reason,created_at "
                    "FROM adjudications WHERE item_id=?", (item["id"],)
                ).fetchone()
                if adj:
                    self.conn.execute(
                        "INSERT INTO snapshot_adjudications"
                        "(freeze_id,item_id,adjudication_id,arbitrator_id,final_label,reason,created_at)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (freeze_id, item["id"], adj["id"], adj["arbitrator_id"],
                         adj["final_label"], adj["reason"], adj["created_at"]),
                    )
                    label, source, adj_id = adj["final_label"], "adjudication", adj["id"]
                else:
                    label, source, adj_id = ann_rows[0]["label"], "consensus", None
                self.conn.execute(
                    "INSERT INTO snapshot_items(freeze_id,item_id,ordinal,text) VALUES(?,?,?,?)",
                    (freeze_id, item["id"], item["ordinal"], item["text"]),
                )
                self.conn.execute(
                    "INSERT INTO gold_records(batch_id,item_id,guideline_id,label,source,adjudication_id,frozen_at)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (batch_id, item["id"], batch["guideline_id"], label, source, adj_id, frozen_at),
                )

            self.conn.execute("UPDATE batches SET status='frozen' WHERE id=?", (batch_id,))

            result = {
                "ok": True,
                "batch_id": batch_id,
                "version": frozen_version,
                "frozen_at": frozen_at,
                "request_id": request_id,
                "metrics": metrics,
            }
            if request_id is not None:
                try:
                    self.conn.execute(
                        "INSERT INTO freeze_requests(request_id,batch_id,response_json,created_at) VALUES(?,?,?,?)",
                        (request_id, batch_id, json.dumps(result, ensure_ascii=False), datetime.now().isoformat()),
                    )
                except sqlite3.IntegrityError:
                    # 同请求号并发（首笔在其它连接刚提交）：复用其首次结果
                    cached = self.conn.execute(
                        "SELECT batch_id,response_json FROM freeze_requests WHERE request_id=?",
                        (request_id,),
                    ).fetchone()
                    if cached and cached["batch_id"] == batch_id:
                        return json.loads(cached["response_json"])
                    raise
            return result

    def export_gold(self, batch_id: int) -> dict:
        batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("批次不存在")
        freeze = self.conn.execute("SELECT * FROM batch_freezes WHERE batch_id=?", (batch_id,)).fetchone()
        if batch["status"] != "frozen" or not freeze:
            raise DomainError("只有已冻结批次可以导出金标准")
        # 导出只认冻结快照：条目文本来自 snapshot_items，不回查活动表
        rows = self.conn.execute(
            "SELECT g.item_id,s.ordinal,s.text,g.label,g.source,g.adjudication_id,g.frozen_at "
            "FROM gold_records g "
            "JOIN snapshot_items s ON s.freeze_id=? AND s.item_id=g.item_id "
            "WHERE g.batch_id=? ORDER BY s.ordinal",
            (freeze["id"], batch_id),
        ).fetchall()
        annotation_count = self.conn.execute(
            "SELECT COUNT(*) FROM snapshot_annotations WHERE freeze_id=?", (freeze["id"],)
        ).fetchone()[0]
        adjudication_count = self.conn.execute(
            "SELECT COUNT(*) FROM snapshot_adjudications WHERE freeze_id=?", (freeze["id"],)
        ).fetchone()[0]
        return {
            "batch_id": batch_id,
            "batch_name": batch["name"],
            "version": freeze["version"],
            "frozen_at": freeze["frozen_at"],
            "metrics": json.loads(freeze["metrics_json"]),
            "snapshot": {"annotations": annotation_count, "adjudications": adjudication_count},
            "records": [dict(row) for row in rows],
        }

    def snapshot(self) -> dict:
        return {
            "users": [dict(r) for r in self.conn.execute("SELECT id,name,role FROM users ORDER BY id")],
            "guidelines": [dict(r) for r in self.conn.execute("SELECT * FROM guidelines ORDER BY id")],
            "batches": [dict(r) for r in self.conn.execute("SELECT * FROM batches ORDER BY id")],
            "items": [dict(r) for r in self.conn.execute("SELECT * FROM items ORDER BY batch_id,ordinal")],
        }
