# 语料标注与争议仲裁

项目使用 Python 标准库、SQLite 和 `http.server`，实现批次、指南版本、重复标注、分歧检测、仲裁、一致性指标、金标准冻结与导出，并以“提交前不可查看含答案讨论”的方式隔离讨论区答案。

## 启动

```bash
python app.py
```

默认地址 <http://127.0.0.1:8112>，默认数据库为 `corpus.db`。首次启动会写入两位标注员、一位仲裁员和一个含分歧的示例批次。

```bash
PORT=9002 CORPUS_DB=/tmp/corpus.db python app.py
```

## 测试

```bash
python -m unittest discover -s tests -v
```

测试包括：分配、标注、发现分歧、阻止提前冻结、仲裁、计算一致性、冻结和导出；批次版本随标注/仲裁推进、过期冻结返回 409 并列出变化、冻结后标注/仲裁返回 409、冻结失败整笔回滚、同一请求号重试返回首次结果、导出只认冻结快照（含并发双管理员冻结与端到端 HTTP 409）；另一条测试验证提交答案前后讨论可见性变化，以及错误角色不能领取标注任务。

## 接口

- `POST /api/users`、`POST /api/guidelines`、`POST /api/batches`
- `POST /api/batches/{id}/items`、`POST /api/batches/{id}/assign`
- `POST /api/annotations`、`POST /api/adjudications`
- `GET /api/items/{id}?user_id=`
- `GET /api/batches/{id}/state`（读取批次状态与 `version`）
- `GET /api/batches/{id}/disagreements`
- `GET /api/batches/{id}/consistency`
- `POST /api/batches/{id}/freeze`（`expected_version`、`request_id`）
- `GET /api/batches/{id}/gold`

一致性同时返回逐条成对一致率和 Fleiss Kappa。冻结要求每条至少有两人标注、没有未仲裁分歧；冻结后不能修改标注，导出结果来自不可变的 `gold_records`。

## 冻结的版本控制与幂等

- 每个批次带有单调递增的 `version`，初始为 1；**每次标注提交/修改、仲裁提交/修改都会推进版本**，并在 `batch_changes` 中留下一条变化记录。
- 冻结采用乐观并发：先 `GET /api/batches/{id}/state` 读到 `version`，再在冻结请求里带 `expected_version`。版本不一致返回 **409**，响应包含 `expected_version`、`current_version` 以及该批次自读取以来的 `changes` 列表。两名管理员同时提交时，只有一人成功，另一人收到 409，绝不会各写一份金标准。
- 冻结后再提交标注或仲裁同样返回 **409**（晚到数据不会落在冻结之后）；标注若先于冻结落地，冻结会因版本过期返回 409，管理员按最新版本重新提交即可。
- 冻结在单个 `BEGIN IMMEDIATE` 事务内把**当时的条目、全部标注、仲裁和一致性指标**写入不可变快照（`batch_freezes`、`snapshot_items`、`snapshot_annotations`、`snapshot_adjudications`、`gold_records`），任何一步失败整笔回滚，不会留下半份金标准。
- 冻结请求可带 `request_id`：同一请求号重试（包括并发重复和拿着旧版本重试）直接返回**首次结果**，不重复冻结；同一请求号用于不同批次返回 400。
- `GET /api/batches/{id}/gold` 只读取冻结快照中的条目文本、标签、仲裁与指标，即使活动表事后被改动，导出仍保持冻结时的内容。
