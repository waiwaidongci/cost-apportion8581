"""启动时结构演进：create_all 负责新表，已存在的旧表用 ADD COLUMN 补齐列。

演示环境无 Alembic，所有变更只做加法（nullable/带默认值），幂等可重复执行。
"""
from sqlalchemy import text,inspect
from src.config.database import engine,Base
from src.models import identity,source,run  # noqa: F401  注册全部模型

_RUN_NEW_COLUMNS={
 'attempts':"INTEGER NOT NULL DEFAULT 0",
 'worker_id':"VARCHAR(120)",
 'lease_expires_at':"FLOAT",
 'heartbeat_at':"FLOAT",
}

def upgrade():
 Base.metadata.create_all(engine)
 inspector=inspect(engine)
 existing={c['name'] for c in inspector.get_columns('allocationruns')}
 with engine.begin() as conn:
  for name,ddl in _RUN_NEW_COLUMNS.items():
   if name not in existing:
    conn.execute(text(f'ALTER TABLE allocationruns ADD COLUMN {name} {ddl}'))
  # 历史遗留的 running/queued 任务在新模型下可被补领：running 且无租约视为租约已过期
  if 'lease_expires_at' not in existing:
   conn.execute(text("UPDATE allocationruns SET lease_expires_at=0 WHERE status IN ('queued','running')"))
