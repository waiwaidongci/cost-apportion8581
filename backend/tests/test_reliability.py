import pytest
from src.models.run import Run,FaultRule
from src.services import tasks
from src.services.faults import InjectedFault,set_rule
from src.config.database import Session
import src.worker as worker
from conftest import make_run

def _get(run_id):
 with Session() as s: return s.get(Run,run_id)

def test_queue_flushed_still_claimed_from_db(db,fake_redis,clock):
 """缓存队列清空也能补领：提交后直接清空 Redis，worker 仍靠数据库扫描完成任务。"""
 run=make_run(db)
 fake_redis.delete('report-jobs')
 assert fake_redis.llen('report-jobs')==0
 worker.run_once('worker-A')
 with Session() as s:
  done=s.get(Run,run.id)
  assert done.status=='completed' and done.attempts==1 and done.worker_id=='worker-A'
  assert done.result['summary']['守恒差额分']==0

def test_claim_fault_then_lease_takeover(db,clock,crash):
 """故障点①取走任务：领取已落盘后崩溃；租约过期由另一执行者接管完成。"""
 run=make_run(db)
 with Session() as admin: set_rule(admin,'claim',True,note='取任务即崩')
 with pytest.raises(InjectedFault): worker.run_once('worker-A')
 mid=_get(run.id)
 assert mid.status=='running' and mid.worker_id=='worker-A' and mid.attempts==1
 assert mid.lease_expires_at>clock.t
 # 租约未过期：他人不能接管
 with Session() as s:
  assert tasks.claim(s,'worker-B') is None
 clock.advance(20)  # 租约 15s 已过期
 with Session() as s:
  job=tasks.claim(s,'worker-B')
  assert job and job.id==run.id and job.attempts==2 and job.worker_id=='worker-B'
  from src.services.calculation import calculate
  j=s.get(Run,run.id)
  assert tasks.finish(s,j,'worker-B',calculate(j.snapshot['sources'],j.snapshot['policy']))
 final=_get(run.id)
 assert final.status=='completed' and final.worker_id=='worker-B' and final.attempts==2
 with Session() as s:
  rule=s.query(FaultRule).filter_by(point='claim').first()
  assert rule.enabled is False and rule.fires==1  # 规则一次性

def test_result_fault_recomputed_by_takeover(db,clock,crash):
 """故障点②保存结果：计算完成、落盘前崩溃；接管者重新计算并完成，沿用同一冻结底稿。"""
 run=make_run(db)
 with Session() as s:
  assert tasks.claim(s,'worker-A')  # 先有效领取
 with Session() as admin: set_rule(admin,'result',True)
 with pytest.raises(InjectedFault):
  worker._run_job(run.id,'worker-A')
 mid=_get(run.id)
 assert mid.status=='running' and mid.result is None  # 结果未保存
 checksum,snapshot=mid.checksum,mid.snapshot
 clock.advance(20)
 with Session() as s:
  assert tasks.claim(s,'worker-B')  # 租约过期接管
 worker.run_once('worker-B')
 final=_get(run.id)
 assert final.status=='completed' and final.worker_id=='worker-B'
 assert final.checksum==checksum and final.snapshot==snapshot  # 口径与底稿不变
 assert final.result['summary']['守恒差额分']==0

def test_stale_owner_late_result_cannot_overwrite(db,clock):
 """旧执行者迟到的结果与失败均无权覆盖新持有者。"""
 run=make_run(db)
 with Session() as s:
  assert tasks.claim(s,'worker-A')
 clock.advance(20)
 with Session() as s:
  assert tasks.claim(s,'worker-B')
 bogus={'summary':{'守恒差额分':-999},'rows':[],'trace':[],'method':'x','warnings':[]}
 with Session() as s:
  j=s.get(Run,run.id)
  assert tasks.finish(s,j,'worker-A',bogus) is False    # 迟到成功结果被拒
  assert tasks.fail(s,j,'worker-A','迟到失败') is False  # 迟到失败也被拒
 with Session() as s:
  after=s.get(Run,run.id)
  assert after.worker_id=='worker-B' and after.status=='running' and after.result is None
  from src.services.calculation import calculate
  assert tasks.finish(s,s.get(Run,run.id),'worker-B',
                      calculate(after.snapshot['sources'],after.snapshot['policy']))
 assert _get(run.id).status=='completed'

def test_parallel_claimers_only_one_holds_lease(db):
 """多个执行者并行领取同一任务，只有一人获得租约。"""
 run=make_run(db)
 with Session() as a, Session() as b:
  ja=tasks.claim(a,'worker-A');jb=tasks.claim(b,'worker-B')
  winners=[x.worker_id for x in (ja,jb) if x]
 assert len(winners)==1
 assert _get(run.id).attempts==1

def test_failure_marked_when_owner_holds_lease(db,monkeypatch):
 """持有者有效租约内的真实计算错误落为 failed，可被复核链路看到。"""
 run=make_run(db)
 def boom(src,pol): raise RuntimeError('口径解析失败')
 monkeypatch.setattr(worker,'calculate',boom)
 worker.run_once('worker-A')
 obj=_get(run.id)
 assert obj.status=='failed' and '口径解析失败' in obj.error and obj.worker_id=='worker-A'
