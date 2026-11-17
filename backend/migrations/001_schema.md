# 初始结构

SQLAlchemy 启动创建以下业务表：costperiods, costentrys, allocationrules, allocationruns, delivery_schedules, deliveries、users、audits。Source 专属 JSON 字段由对应 validation 严格校验；Policy 为不可覆盖的版本记录；Run 保存 source/policy 冻结快照；Period 仅用于已有期间选择器，不计入核心复杂度。已有数据库升级需先备份，后续字段演进应以独立迁移执行。

# 002 可靠核算与交付（租约接管 + outbox）

## 变更
- `allocationruns` 新增列：`attempts`（累计领取/接管次数，默认 0）、`worker_id`（当前持有者）、`lease_expires_at`（租约到期时间戳）、`heartbeat_at`（最近心跳）。均为加法变更，nullable 或带默认值。
- 新表 `delivery_outbox`：待交付事件，字段 schedule_id/run_id/due_key(唯一)/due_at/status(pending|sent)/attempts/last_error/created/sent_at。
- 新表 `fault_rules`：故障注入开关，point 唯一（claim/result/deliver），一次性触发后自动 enabled=false。

## 升级方式
应用与 worker 启动时 `src.config.migrate.upgrade()` 自动执行：`create_all` 建新表；对已存在的旧 `allocationruns` 用 `ALTER TABLE ADD COLUMN` 补列（幂等，重复执行安全），并把历史 queued/running 任务的租约置为已过期，使其可被补领。PostgreSQL 16 与 SQLite 均验证。

## 语义
- 任务权威在数据库；Redis 列表仅作唤醒提示，消息丢失/清空不影响补领。
- 领取为单条条件 UPDATE（status∈queued/running 且租约为空或过期），多执行者并行只有一人 rowcount=1。
- 租约过期可被任意执行者接管，attempts+1，沿用 Run 最初冻结的 snapshot/checksum，不重读源表。
- 完成/失败的 UPDATE 带 `worker_id=当前持有者 AND status='running'` 条件，旧执行者迟到结果无权覆盖。
- 计划推进与 outbox 事件同事务；投递按 due_key 幂等，唯一约束兜底，同一到期点并发只生成一份 Delivery。
