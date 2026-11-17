"""定时计划与站内交付。

可靠性设计：
1. 计划推进与 DeliveryOutbox 待交付事件在同一事务落盘（崩溃则整体回滚/整体可重放）；
   多执行者用 SELECT ... FOR UPDATE SKIP LOCKED 串行化同一计划。
2. 投递阶段把 outbox 事件写为 Delivery，due_key 唯一约束兜底：重复执行不新增交付。
3. 同一到期计划并发领取只生成一份交付，固定引用原报告（run_id 与快照不变）。
"""
import time
from fastapi import HTTPException
from src.config.database import Session
from src.models.run import Run,Schedule,Delivery,DeliveryOutbox
from src.services.audit import log
from src.services.faults import trigger

def add_schedule(db,user,body):
 run=db.get(Run,body.run_id)
 if not run or not run.approved: raise HTTPException(409,'定时交付需引用已复核报告')
 obj=Schedule(name=body.name,run_id=run.id,interval_seconds=body.interval_seconds,next_at=time.time()+body.interval_seconds,enabled=True)
 db.add(obj);log(db,user.email,'schedule.create',body.name);db.commit();return obj

def _payload(run):
 return {'name':run.name,'checksum':run.checksum,'result':run.result,'review':run.review,'policy':run.snapshot['policy']}

def sweep_schedules(db,worker='worker'):
 """扫描到期计划：每轮把每个到期计划推进一个周期，并登记一条待交付事件。"""
 now=time.time()
 due=db.query(Schedule).filter(Schedule.enabled==True,Schedule.next_at<=now)\
  .order_by(Schedule.id).with_for_update(skip_locked=True).all()
 for schedule in due:
  run=db.get(Run,schedule.run_id)
  if not (run and run.approved and run.status=='completed'):
   schedule.next_at=now+schedule.interval_seconds  # 报告不可用则顺延，不产生交付
   continue
  due_key=f'{schedule.id}:{schedule.next_at}'
  # 同事务：事件落盘 + 计划推进；崩溃一起回滚，重放不会跳过该到期点
  if not db.query(DeliveryOutbox).filter_by(due_key=due_key).first() and \
     not db.query(Delivery).filter_by(due_key=due_key).first():
   db.add(DeliveryOutbox(schedule_id=schedule.id,run_id=run.id,due_key=due_key,due_at=schedule.next_at,
                         status='pending',attempts=0,created=now))
   log(db,worker,'delivery.staged',due_key)
  schedule.next_at=now+schedule.interval_seconds
 db.commit()

def _mark_sent(event_id):
 with Session() as fresh:
  ev=fresh.get(DeliveryOutbox,event_id)
  if ev and ev.status!='sent':
   ev.status='sent';ev.sent_at=time.time();ev.last_error=None;fresh.commit()

def deliver_pending(db,worker='worker',limit=20):
 """重放待交付事件；故障点③在投递提交前触发，重启后幂等重试。"""
 events=db.query(DeliveryOutbox).filter(DeliveryOutbox.status=='pending')\
  .order_by(DeliveryOutbox.id).limit(limit).all()
 for event in events:
  event.attempts=(event.attempts or 0)+1
  run=db.get(Run,event.run_id)
  if not run or run.status!='completed':
   event.last_error='引用报告不可用，保留待重试';db.commit();continue
  # 故障点③「写成交付」：事件已持久化，deliveries 写入前崩溃
  trigger(db,'deliver',{'worker':worker,'target':event.due_key})
  try:
   if not db.query(Delivery).filter_by(due_key=event.due_key).first():
    db.add(Delivery(schedule_id=event.schedule_id,run_id=run.id,due_key=event.due_key,
                    created=time.time(),payload=_payload(run)))
    db.flush()  # 让唯一约束冲突在本事务内暴露
   event.status='sent';event.sent_at=time.time();event.last_error=None
   log(db,worker,'delivery.created',event.due_key)
   db.commit()
  except Exception as exc:
   db.rollback()
   # 并发下另一执行者已写入：用独立会话确认并标记已投递，绝不新增第二份
   with Session() as probe:
    if probe.query(Delivery).filter_by(due_key=event.due_key).first():
     _mark_sent(event.id)
    else:
     with Session() as retry:
      ev=retry.get(DeliveryOutbox,event.id)
      if ev: ev.last_error=str(exc)[:500];retry.commit()

def tick(db,worker='worker'):
 sweep_schedules(db,worker)
 deliver_pending(db,worker)
