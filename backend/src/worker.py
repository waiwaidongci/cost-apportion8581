"""核算执行者。

以数据库任务行为权威，Redis 仅作低延迟唤醒：
- 每次循环先 lpop 提示（可丢失），再无条件扫描数据库补领，缓存队列清空也不丢任务；
- 租约由条件 UPDATE 授予，过期后任意执行者可接管；
- 计算期间持续续租；提交结果时若租约已易主，迟到结果/失败无权覆盖，直接丢弃；
- 交付事件走 outbox 重放，进程任何时点崩溃都可由后续执行者恢复。
可水平扩容多副本并行：docker compose up -d --scale worker=N（共用 db/redis）。
"""
import time,traceback,logging
from pathlib import Path
import redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.config.migrate import upgrade
from src.models import identity,source,run  # noqa: F401
from src.models.run import Run
from src.services.calculation import calculate
from src.services.delivery import tick
from src.services import tasks
from src.services.faults import worker_identity

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
log=logging.getLogger('worker')
upgrade()
WORKER=worker_identity()
r=redis.Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2)
log.info('worker 启动 id=%s',WORKER)

def _drain_hint():
 try:
  for _ in range(5):
   if r.lpop('report-jobs') is None: break
 except Exception: pass  # Redis 不可用不影响数据库补领

def _run_job(run_id,worker):
 # 独立短会话：领取、心跳、提交各自开新会话，避免 ORM 身份映射缓存条件 UPDATE 后的旧值
 with Session() as db:
  job=db.get(Run,run_id)
  if not job or job.worker_id!=worker or job.status!='running': return
  snapshot=job.snapshot  # 接管沿用最初冻结的底稿与口径，不重新读取源表
  tasks.heartbeat(db,job,worker)  # 开始计算前续租，长任务不被误判过期
 try:
  result=calculate(snapshot['sources'],snapshot['policy'])
 except Exception as exc:
  traceback.print_exc()
  with Session() as db:
   fresh=db.get(Run,run_id)
   if fresh.worker_id==worker and fresh.status=='running':
    tasks.fail(db,fresh,worker,str(exc))
   else:
    log.warning('任务 %s 迟到失败无权覆盖持有者状态',run_id)
  return
 with Session() as db:
  fresh=db.get(Run,run_id)
  # 提交前确认租约仍属于自己：已被接管则迟到结果直接丢弃，无权覆盖
  if fresh.worker_id!=worker or fresh.status!='running':
   log.warning('任务 %s 已被接管，本执行者迟到结果丢弃',run_id);return
  if tasks.finish(db,fresh,worker,result):
   log.info('任务 %s 完成 attempts=%s',run_id,fresh.attempts)
  else:
   log.warning('任务 %s 完成提交未被接受（租约易主），结果丢弃',run_id)

def run_once(worker=WORKER):
 """一轮处理：提示排空→数据库补领→核算→交付重放。供循环与测试共用。"""
 _drain_hint()
 claimed=None
 with Session() as db:
  if tasks.has_pending(db):
   job=tasks.claim(db,worker)  # 命中 claim 故障点会在该进程内退出
   if job: claimed=job.id;log.info('领取任务 %s attempts=%s by %s',job.id,job.attempts,worker)
 if claimed is not None:
  _run_job(claimed,worker)
 else:
  # 本执行者已持有（其他会话领取或上轮续租）的运行任务也要继续推进
  with Session() as db:
   held=db.query(Run.id).filter_by(worker_id=worker,status='running').first()
  if held: _run_job(held[0],worker)
 with Session() as db: tick(db,worker)

if __name__=='__main__':
 while True:
  Path('/tmp/worker-heartbeat').write_text(str(time.time()))
  try: run_once()
  except Exception: traceback.print_exc()
  time.sleep(1)
