import pytest
from src.models.run import Run,Schedule,Delivery,DeliveryOutbox
from src.services import delivery
from src.services.faults import InjectedFault,set_rule
from src.config.database import Session
from conftest import make_completed_approved,make_schedule

def _counts():
 with Session() as s:
  return (s.query(Delivery).count(),s.query(DeliveryOutbox).count(),
          s.query(DeliveryOutbox).filter_by(status='pending').count())

def test_outbox_then_delivery_happy_path(db,clock):
 run=make_completed_approved(db)
 sch=make_schedule(db,run)
 first_due=sch.next_at
 clock.advance(61)  # 首次到期
 delivery.tick(db,'worker-A')
 d,ob,pending=_counts()
 assert ob==1 and pending==0 and d==1
 with Session() as s:
  event=s.query(DeliveryOutbox).first();d0=s.query(Delivery).first()
  assert event.due_key==d0.due_key==f'{sch.id}:{first_due}'
  assert d0.run_id==run.id and d0.payload['checksum']==run.checksum  # 固定引用原报告

def test_deliver_fault_replayed_without_duplicate(db,clock,crash):
 """故障点③写成交付：事件已落盘、deliveries 写入前崩溃；重放后仍只有一份。"""
 run=make_completed_approved(db);make_schedule(db,run)
 clock.advance(61)
 delivery.sweep_schedules(db,'worker-A')
 assert _counts()==(0,1,1)
 with Session() as admin: set_rule(admin,'deliver',True)
 with pytest.raises(InjectedFault):
  with Session() as s: delivery.deliver_pending(s,'worker-A')
 assert _counts()==(0,1,1)  # 崩溃时无交付，事件仍待投递
 for _ in range(3):         # 多次重放
  with Session() as s: delivery.deliver_pending(s,'worker-B')
 d,ob,pending=_counts()
 assert (d,ob,pending)==(1,1,0)  # 恰好一份，事件转 sent

def test_repeated_ticks_keep_single_delivery_per_due(db,clock):
 """重复执行不新增交付：同一到期点多次 tick 幂等，跨周期才各产生一份。"""
 run=make_completed_approved(db);sch=make_schedule(db,run)
 clock.advance(61)
 for _ in range(3): delivery.tick(db,'worker-A')  # 时钟不动：同一到期点
 assert _counts()==(1,1,0)
 clock.advance(61);delivery.tick(db,'worker-A')  # 下一周期
 assert _counts()==(2,2,0)
 with Session() as s:
  keys=[x.due_key for x in s.query(Delivery).all()]
  assert len(set(keys))==2
  assert all(x.run_id==run.id for x in s.query(Delivery).all())

def test_concurrent_same_due_schedule_single_delivery(db,clock):
 """同一到期计划被并发领取时也只生成一份：两执行者并行 sweep+deliver。"""
 run=make_completed_approved(db);sch=make_schedule(db,run)
 clock.advance(61)
 with Session() as a, Session() as b:
  delivery.tick(a,'worker-A');delivery.tick(b,'worker-B')
 assert _counts()==(1,1,0)

def test_replay_after_crash_between_stage_and_commit(db,clock):
 """模拟 sweep 提交后进程崩溃：重启重放同一轮不产生重复事件/交付。"""
 run=make_completed_approved(db);sch=make_schedule(db,run)
 clock.advance(61)
 delivery.sweep_schedules(db,'worker-A')
 due_next=db.get(Schedule,sch.id).next_at
 with Session() as s:
  delivery.sweep_schedules(s,'worker-A')  # 未到下一点，无新增
  delivery.deliver_pending(s,'worker-A')
 assert db.get(Schedule,sch.id).next_at==due_next
 assert _counts()==(1,1,0)

def test_delivery_fixed_to_original_report_snapshot(db,clock):
 """交付固定引用原报告：之后源数据变化不影响已生成与将生成的交付内容。"""
 run=make_completed_approved(db);make_schedule(db,run)
 clock.advance(61);delivery.tick(db,'worker-A')
 with Session() as s:
  first=s.query(Delivery).first().payload
 from src.models.source import Source
 src=db.query(Source).first();src.data['amount_cents']+=12345;db.commit()
 clock.advance(61);delivery.tick(db,'worker-A')
 with Session() as s:
  for d in s.query(Delivery).all():
   assert d.payload['checksum']==run.checksum and d.payload['result']==first['result']

def test_concurrent_duplicate_due_key_marks_sent_without_second_row(db,clock):
 """并发兜底：另一执行者已写入同 due_key 的 Delivery 时，重放只标记 sent，不新增第二份。"""
 run=make_completed_approved(db);sch=make_schedule(db,run)
 clock.advance(61)
 delivery.sweep_schedules(db,'worker-A')
 with Session() as s:
  ev=s.query(DeliveryOutbox).first()
  # 模拟竞争者已抢先落盘同键交付
  s.add(Delivery(schedule_id=sch.id,run_id=run.id,due_key=ev.due_key,created=clock.t,payload={'x':1}))
  s.commit();key=ev.due_key
 with Session() as s: delivery.deliver_pending(s,'worker-B')
 with Session() as s:
  assert s.query(Delivery).filter_by(due_key=key).count()==1
  assert s.query(DeliveryOutbox).filter_by(due_key=key,status='sent').count()==1
