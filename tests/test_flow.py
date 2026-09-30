import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import ConflictError, CorpusDB, DomainError


class CorpusFlowTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = CorpusDB(self.path)
        self.a1 = self.db.add_user("甲", "annotator")
        self.a2 = self.db.add_user("乙", "annotator")
        self.arb = self.db.add_user("仲裁", "arbitrator")
        self.mgr = self.db.add_user("管理", "manager")
        self.g = self.db.add_guideline("v1", "独立标注")
        self.batch = self.db.create_batch("测试批次", self.g)
        self.item1 = self.db.add_item(self.batch, 1, "这个版本很快。")
        self.item2 = self.db.add_item(self.batch, 2, "没有明显变化。")

    def tearDown(self):
        self.db.close()
        os.unlink(self.path)

    def _freeze_ready(self, adjudicate_item1=True):
        for item in (self.item1, self.item2):
            self.db.assign(item, self.a1)
            self.db.assign(item, self.a2)
        self.db.submit_annotation(self.item1, self.a1, "正向")
        self.db.submit_annotation(self.item1, self.a2, "中性")
        self.db.submit_annotation(self.item2, self.a1, "中性")
        self.db.submit_annotation(self.item2, self.a2, "中性")
        if adjudicate_item1:
            self.db.adjudicate(self.item1, "正向", "速度描述构成明确正向倾向", self.arb)

    def test_full_annotation_disagreement_adjudication_freeze_flow(self):
        for item in (self.item1, self.item2):
            self.db.assign(item, self.a1)
            self.db.assign(item, self.a2)
        self.db.submit_annotation(self.item1, self.a1, "正向")
        self.db.submit_annotation(self.item1, self.a2, "中性")
        self.db.submit_annotation(self.item2, self.a1, "中性")
        self.db.submit_annotation(self.item2, self.a2, "中性")
        self.assertEqual(1, len(self.db.disagreements(self.batch)))
        with self.assertRaisesRegex(DomainError, "分歧"):
            self.db.freeze_batch(self.batch, self.mgr)
        self.db.adjudicate(self.item1, "正向", "速度描述构成明确正向倾向", self.arb)
        result = self.db.freeze_batch(self.batch, self.mgr)
        self.assertIsNotNone(result["metrics"]["pairwise_agreement"])
        exported = self.db.export_gold(self.batch)
        self.assertEqual(2, len(exported["records"]))
        self.assertEqual("adjudication", exported["records"][0]["source"])

    def test_legacy_precheck_still_blocks_freeze_with_open_disagreement(self):
        self._freeze_ready(adjudicate_item1=False)
        with self.assertRaisesRegex(DomainError, "分歧"):
            self.db.freeze_batch(self.batch, self.mgr)
        self.db.adjudicate(self.item1, "正向", "速度描述构成明确正向倾向", self.arb)
        result = self.db.freeze_batch(self.batch, self.mgr)
        self.assertTrue(result["ok"])

    def test_answer_isolation_and_role_validation(self):
        self.db.assign(self.item1, self.a1)
        self.db.assign(self.item1, self.a2)
        self.db.add_discussion(self.item1, self.a2, "我认为是正向", True)
        secret = self.db.get_item_for_user(self.item1, self.a1)
        self.assertTrue(secret["discussions"][0]["hidden"])
        self.db.submit_annotation(self.item1, self.a1, "负向")
        visible = self.db.get_item_for_user(self.item1, self.a1)
        self.assertFalse(visible["discussions"][0].get("hidden", False))
        with self.assertRaisesRegex(DomainError, "标注员"):
            self.db.assign(self.item2, self.arb)


class BatchVersionTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = CorpusDB(self.path)
        self.a1 = self.db.add_user("甲", "annotator")
        self.a2 = self.db.add_user("乙", "annotator")
        self.arb = self.db.add_user("仲裁", "arbitrator")
        self.mgr = self.db.add_user("管理", "manager")
        self.g = self.db.add_guideline("v1", "独立标注")
        self.batch = self.db.create_batch("版本批次", self.g)
        self.item1 = self.db.add_item(self.batch, 1, "文本一")
        self.item2 = self.db.add_item(self.batch, 2, "文本二")
        for item in (self.item1, self.item2):
            self.db.assign(item, self.a1)
            self.db.assign(item, self.a2)

    def tearDown(self):
        self.db.close()
        os.unlink(self.path)

    def _annotate_all(self):
        self.db.submit_annotation(self.item1, self.a1, "正向")
        self.db.submit_annotation(self.item1, self.a2, "正向")
        self.db.submit_annotation(self.item2, self.a1, "中性")
        self.db.submit_annotation(self.item2, self.a2, "中性")

    def test_version_advances_on_annotation_and_adjudication(self):
        self.assertEqual(1, self.db.get_batch(self.batch)["version"])
        self.db.submit_annotation(self.item1, self.a1, "正向")
        self.assertEqual(2, self.db.get_batch(self.batch)["version"])
        self.db.submit_annotation(self.item1, self.a2, "中性")
        self.assertEqual(3, self.db.get_batch(self.batch)["version"])
        # 修改标注同样推进版本
        self.db.submit_annotation(self.item1, self.a2, "正向")
        self.assertEqual(4, self.db.get_batch(self.batch)["version"])
        self.db.submit_annotation(self.item2, self.a1, "中性")
        self.db.submit_annotation(self.item2, self.a2, "中性")
        before = self.db.get_batch(self.batch)["version"]
        self.db.adjudicate(self.item1, "正向", "语境明确为正向", self.arb)
        self.assertEqual(before + 1, self.db.get_batch(self.batch)["version"])

    def test_stale_freeze_version_returns_conflict_and_lists_changes(self):
        self._annotate_all()
        read_version = self.db.get_batch(self.batch)["version"]
        # 管理员读取版本后，又有人修改了标注（并重新仲裁）
        self.db.submit_annotation(self.item1, self.a2, "中性")
        self.db.submit_annotation(self.item1, self.a2, "正向")
        self.db.adjudicate(self.item1, "正向", "语境明确为正向", self.arb)
        current_version = self.db.get_batch(self.batch)["version"]
        with self.assertRaises(ConflictError) as ctx:
            self.db.freeze_batch(self.batch, self.mgr, expected_version=read_version)
        details = ctx.exception.details
        self.assertEqual(read_version, details["expected_version"])
        self.assertEqual(current_version, details["current_version"])
        kinds = [c["kind"] for c in details["changes"]]
        self.assertIn("annotation_updated", kinds)
        self.assertIn("adjudication_submitted", kinds)
        self.assertTrue(all(c["version"] > read_version for c in details["changes"]))
        # 批次没有被冻结，没有半成品
        self.assertEqual("annotating", self.db.get_batch(self.batch)["status"])
        self.assertEqual(0, self.db.conn.execute("SELECT COUNT(*) FROM gold_records WHERE batch_id=?", (self.batch,)).fetchone()[0])
        self.assertEqual(0, self.db.conn.execute("SELECT COUNT(*) FROM batch_freezes WHERE batch_id=?", (self.batch,)).fetchone()[0])

    def test_fresh_version_freezes(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        result = self.db.freeze_batch(self.batch, self.mgr, expected_version=version)
        self.assertEqual(version, result["version"])
        self.assertEqual("frozen", self.db.get_batch(self.batch)["status"])

    def test_post_freeze_annotation_and_adjudication_conflict(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        self.db.freeze_batch(self.batch, self.mgr, expected_version=version)
        # 冻结后标注一律拒绝，即使内容与之前相同
        with self.assertRaises(ConflictError) as ann_ctx:
            self.db.submit_annotation(self.item2, self.a1, "中性")
        self.assertEqual(version, ann_ctx.exception.details["current_version"])
        with self.assertRaises(ConflictError):
            self.db.adjudicate(self.item1, "中性", "冻结后改判不应生效", self.arb)
        # 再冻结也是 409
        with self.assertRaises(ConflictError):
            self.db.freeze_batch(self.batch, self.mgr, expected_version=version)

    def test_annotation_before_freeze_wins_and_makes_freeze_stale(self):
        # 管理员读到版本 v 之后、提交冻结之前，标注先落地：冻结必须 409
        self.db.submit_annotation(self.item1, self.a1, "正向")
        self.db.submit_annotation(self.item1, self.a2, "正向")
        self.db.submit_annotation(self.item2, self.a1, "中性")
        read_version = self.db.get_batch(self.batch)["version"]
        # 晚到的第二份标注（有效，推进版本）
        self.db.submit_annotation(self.item2, self.a2, "中性")
        with self.assertRaises(ConflictError) as ctx:
            self.db.freeze_batch(self.batch, self.mgr, expected_version=read_version)
        self.assertEqual(read_version + 1, ctx.exception.details["current_version"])
        # 用新版本重试即可冻结
        result = self.db.freeze_batch(
            self.batch, self.mgr,
            expected_version=read_version + 1, request_id="req-1",
        )
        self.assertTrue(result["ok"])

    def test_idempotent_retry_same_request_id_returns_first_result(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        first = self.db.freeze_batch(self.batch, self.mgr, expected_version=version, request_id="req-abc")
        self.assertEqual("req-abc", first["request_id"])
        # 同请求号重试：带不带版本都返回首次结果
        retry = self.db.freeze_batch(self.batch, self.mgr, expected_version=version, request_id="req-abc")
        stale_retry = self.db.freeze_batch(self.batch, self.mgr, expected_version=999, request_id="req-abc")
        for again in (retry, stale_retry):
            self.assertEqual(first["frozen_at"], again["frozen_at"])
            self.assertEqual(first["version"], again["version"])
        # 只有一份快照、一份金标准
        self.assertEqual(
            1, self.db.conn.execute("SELECT COUNT(*) FROM batch_freezes WHERE batch_id=?", (self.batch,)).fetchone()[0]
        )
        self.assertEqual(
            2, self.db.conn.execute("SELECT COUNT(*) FROM gold_records WHERE batch_id=?", (self.batch,)).fetchone()[0]
        )
        # 同号用于别的批次 -> 400
        other = self.db.create_batch("另一个批次", self.g)
        with self.assertRaisesRegex(DomainError, "其他批次"):
            self.db.freeze_batch(other, self.mgr, expected_version=1, request_id="req-abc")

    def test_failed_freeze_rolls_back_entire_snapshot(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        # 在写入 gold 快照的中途让事务失败：任何半成品都必须随事务回滚
        self.db.conn.execute(
            "CREATE TEMP TRIGGER fail_halfway BEFORE INSERT ON snapshot_items BEGIN"
            "  SELECT RAISE(ABORT,'injected freeze failure');"
            " END"
        )
        with self.assertRaises(Exception):
            self.db.freeze_batch(self.batch, self.mgr, expected_version=version, request_id="req-fail")
        self.db.conn.execute("DROP TRIGGER fail_halfway")
        for table in ("gold_records", "batch_freezes"):
            count = self.db.conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch,)
            ).fetchone()[0]
            self.assertEqual(0, count, f"{table} 残留了半份数据")
        for table in ("snapshot_items", "snapshot_annotations", "snapshot_adjudications"):
            count = self.db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(0, count, f"{table} 残留了半份数据")
        pending = self.db.conn.execute(
            "SELECT COUNT(*) FROM freeze_requests WHERE request_id='req-fail'"
        ).fetchone()[0]
        self.assertEqual(0, pending, "失败请求不应该留下幂等记录")
        # 批次仍是可标注状态、版本没变，可以用同一请求号重来
        self.assertEqual("annotating", self.db.get_batch(self.batch)["status"])
        self.assertEqual(version, self.db.get_batch(self.batch)["version"])
        retry = self.db.freeze_batch(self.batch, self.mgr, expected_version=version, request_id="req-fail")
        self.assertTrue(retry["ok"])

    def test_export_only_reads_frozen_snapshot(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        self.db.freeze_batch(self.batch, self.mgr, expected_version=version, request_id="req-exp")
        # 冻结后直接绕过领域接口篡改活动表；导出必须只认快照
        self.db.conn.execute("UPDATE items SET text='被改过的文本' WHERE id=?", (self.item1,))
        self.db.conn.execute("UPDATE annotations SET label='负向' WHERE item_id=?", (self.item1,))
        exported = self.db.export_gold(self.batch)
        self.assertEqual(version, exported["version"])
        first = next(r for r in exported["records"] if r["item_id"] == self.item1)
        self.assertEqual("文本一", first["text"])
        self.assertEqual("正向", first["label"])
        self.assertEqual(4, exported["snapshot"]["annotations"])
        # 快照表里保留的是冻结时的标注与文本
        snap = self.db.conn.execute(
            "SELECT label FROM snapshot_annotations WHERE freeze_id="
            "(SELECT id FROM batch_freezes WHERE batch_id=?) AND item_id=?",
            (self.batch, self.item1),
        ).fetchall()
        self.assertTrue(snap)
        self.assertTrue({r["label"] for r in snap} <= {"正向"})

    def test_concurrent_freeze_only_one_gold_standard(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        results: list[Exception | dict] = []
        barrier = threading.Barrier(2)

        def manager(name):
            try:
                barrier.wait()
                results.append(self.db.freeze_batch(
                    self.batch, self.mgr, expected_version=version, request_id=name
                ))
            except Exception as exc:  # noqa: BLE001 - 收集到主线程断言
                results.append(exc)

        t1 = threading.Thread(target=manager, args=("req-a",))
        t2 = threading.Thread(target=manager, args=("req-b",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(2, len(results))
        successes = [r for r in results if isinstance(r, dict)]
        conflicts = [r for r in results if isinstance(r, ConflictError)]
        self.assertEqual(1, len(successes))
        self.assertEqual(1, len(conflicts))
        self.assertEqual(
            1, self.db.conn.execute("SELECT COUNT(*) FROM batch_freezes WHERE batch_id=?", (self.batch,)).fetchone()[0]
        )
        self.assertEqual(
            2, self.db.conn.execute("SELECT COUNT(*) FROM gold_records WHERE batch_id=?", (self.batch,)).fetchone()[0]
        )

    def test_concurrent_same_request_id_both_get_first_result(self):
        self._annotate_all()
        version = self.db.get_batch(self.batch)["version"]
        results: list[Exception | dict] = []
        barrier = threading.Barrier(2)

        def retry_freeze():
            try:
                barrier.wait()
                results.append(self.db.freeze_batch(
                    self.batch, self.mgr, expected_version=version, request_id="req-dup"
                ))
            except Exception as exc:  # noqa: BLE001
                results.append(exc)

        t1 = threading.Thread(target=retry_freeze)
        t2 = threading.Thread(target=retry_freeze)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(2, len(results))
        self.assertTrue(all(isinstance(r, dict) for r in results), results)
        self.assertEqual(results[0]["frozen_at"], results[1]["frozen_at"])
        self.assertEqual(
            1, self.db.conn.execute("SELECT COUNT(*) FROM freeze_requests WHERE request_id='req-dup'").fetchone()[0]
        )


class FreezeHttpTest(unittest.TestCase):
    """端到端验证 409 状态码与幂等响应。"""

    @classmethod
    def setUpClass(cls):
        fd, cls.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.environ["CORPUS_DB"] = cls.path
        import app as app_module
        cls.app_module = app_module
        cls.db = app_module.Handler.db
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app_module.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.a1 = cls.db.add_user("http-甲", "annotator")
        cls.a2 = cls.db.add_user("http-乙", "annotator")
        cls.mgr = cls.db.add_user("http-管理", "manager")
        cls.g = cls.db.add_guideline("http-v1", "规则")
        cls.batch = cls.db.create_batch("http 批次", cls.g)
        cls.item = cls.db.add_item(cls.batch, 1, "文本")
        cls.db.assign(cls.item, cls.a1)
        cls.db.assign(cls.item, cls.a2)
        cls.db.submit_annotation(cls.item, cls.a1, "正向")
        cls.db.submit_annotation(cls.item, cls.a2, "正向")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join()
        cls.server.server_close()
        cls.db.close()
        os.unlink(cls.path)
        for suffix in ("-wal", "-shm"):
            p = cls.path + suffix
            if os.path.exists(p):
                os.unlink(p)

    def _request(self, method, path, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_version_conflict_and_idempotency_over_http(self):
        status, state = self._request("GET", f"/api/batches/{self.batch}/state")
        self.assertEqual(200, status)
        version = state["version"]

        # 标注推进版本后，旧版本冻结 -> 409 且列出变化
        self.db.submit_annotation(self.item, self.a1, "正向", "改个备注")
        code, body = self._request(
            "POST", f"/api/batches/{self.batch}/freeze",
            {"manager_id": self.mgr, "expected_version": version, "request_id": "http-req-1"},
        )
        self.assertEqual(409, code)
        self.assertFalse(body["ok"])
        self.assertEqual(version + 1, body["current_version"])
        self.assertEqual("annotation_updated", body["changes"][0]["kind"])

        # 新版本冻结成功；冻结后标注 -> 409
        code, first = self._request(
            "POST", f"/api/batches/{self.batch}/freeze",
            {"manager_id": self.mgr, "expected_version": version + 1, "request_id": "http-req-1"},
        )
        self.assertEqual(200, code)
        self.assertTrue(first["ok"])
        code, body = self._request(
            "POST", "/api/annotations",
            {"item_id": self.item, "annotator_id": self.a2, "label": "正向"},
        )
        self.assertEqual(409, code)

        # 同请求号重试返回首次结果（哪怕带的是旧版本）
        code, retry = self._request(
            "POST", f"/api/batches/{self.batch}/freeze",
            {"manager_id": self.mgr, "expected_version": version, "request_id": "http-req-1"},
        )
        self.assertEqual(200, code)
        self.assertEqual(first["frozen_at"], retry["frozen_at"])


if __name__ == "__main__":
    unittest.main()
