"""核算任务的领取与落盘。

权威状态在数据库（allocationruns），Redis 队列仅作唤醒提示：
- 队列消息丢失或被清空时，worker 仍可按 status+租约扫描补领；
- 领取使用单条条件 UPDATE 原子完成，多执行者并行只有一人成功；
- 租约过期后任何执行者均可接管，attempts+1、换持有者，沿用最初冻结的 snapshot/checksum；
- 完成/失败均带租约归属条件更新，旧执行者迟到的结果与失败无权覆盖新持有者的状态。
"""
import time
from sqlalchemy import update,and_,or_
from sqlalchemy.orm import Session
from src.models.run import Run
from src.services.faults import lease_seconds,trigger

def _claimable(now):
 # queued 立即可领；running 仅当租约过期（lease_expires_at 为空或已到期）才可被接管
 return and_(
  Run.status.in_(('queued','running')),
  or_(Run.lease_expires_at.is_(None),Run.lease_expires_at<=now),
 )

def has_pending(db):
 now=time.time()
 return db.query(Run.id).filter(_claimable(now)).first() is not None

def claim(db:Session,worker:str):
 """原子领取一个可执行任务；无任务返回 None。

 以单条条件 UPDATE 的 rowcount 作为唯一仲裁：多执行者并行时，数据库行锁串行化
 UPDATE，WHERE 不匹配（他人有效租约）则 rowcount=0，SELECT 仅用于挑选候选 id。
 """
 now=time.time();lease=lease_seconds()
 # 候选 id（不加行锁；真正的互斥由下面的条件 UPDATE 承担）
 row=db.query(Run.id).filter(_claimable(now)).order_by(Run.id).first()
 if not row: return None
 run_id=row[0]
 result=db.execute(update(Run).where(Run.id==run_id,_claimable(now)).values(
  status='running',worker_id=worker,lease_expires_at=now+lease,heartbeat_at=now,attempts=Run.attempts+1))
 db.commit()
 if result.rowcount!=1:
  return None
 db.expire_all()  # 丢弃身份映射缓存，确保重新读取到刚提交的 worker_id/租约
 job=db.get(Run,run_id)
 # 故障点①「取走任务」：领取已提交（其他执行者可见 running+租约），随即崩溃
 trigger(db,'claim',{'worker':worker,'target':run_id})
 return job

def heartbeat(db:Session,job:Run,worker:str):
 now=time.time()
 db.execute(update(Run).where(Run.id==job.id,Run.worker_id==worker).values(heartbeat_at=now,lease_expires_at=now+lease_seconds()))
 db.commit()

def finish(db:Session,job:Run,worker:str,result):
 """仅当前租约持有者可写成功结果；旧持有者迟到提交返回 False（无权覆盖）。"""
 now=time.time()
 # 故障点②「保存结果」：计算已完成，结果尚未落盘即崩溃，接管者将重新计算
 trigger(db,'result',{'worker':worker,'target':job.id})
 stmt=update(Run).where(Run.id==job.id,Run.worker_id==worker,Run.status=='running').values(
  status='completed',result=result,error=None,lease_expires_at=None,heartbeat_at=now)
 res=db.execute(stmt);db.commit();return res.rowcount==1

def fail(db:Session,job:Run,worker:str,error:str):
 """失败同样受租约归属保护；接管次数耗尽前保留可补领状态。"""
 now=time.time()
 stmt=update(Run).where(Run.id==job.id,Run.worker_id==worker,Run.status=='running').values(
  status='failed',error=error,lease_expires_at=None,heartbeat_at=now)
 res=db.execute(stmt);db.commit();return res.rowcount==1
