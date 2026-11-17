"""真实多进程崩溃-接管演练：独立 worker 进程，SQLite 库；Redis 端口不可用也照常补领。

覆盖三个故障注入时点：
  ① claim   取走任务后崩溃 → 租约过期由另一执行者接管完成
  ② result  保存结果前崩溃 → 接管者沿用冻结底稿重新计算
  ③ deliver 写成交付前崩溃 → outbox 重放，同一到期点只产生一份交付
用法：python tests/e2e_demo.py
"""
import os,pathlib,subprocess,sys,time
DB='/tmp/cost-e2e.db'
pathlib.Path(DB).unlink(missing_ok=True)
os.environ.update(DATABASE_URL=f'sqlite:///{DB}',JWT_SECRET='t',
                  PROJECT_JSON='/workspace/backend/project.json',WORKER_LEASE_SECONDS='3',
                  REDIS_URL='redis://127.0.0.1:6390/0')
ENV=dict(os.environ,PYTHONPATH='/tmp/pylibs:/workspace/backend')
sys.path.insert(0,'/tmp/pylibs');sys.path.insert(0,'/workspace/backend')

from src.config.migrate import upgrade  # noqa: E402
upgrade()
from src.config.database import Session  # noqa: E402
from src.models.identity import User  # noqa: E402
from src.models.source import Source,Policy  # noqa: E402
from src.models.run import Run,FaultRule,Delivery,DeliveryOutbox  # noqa: E402
from src.utils.security import password_hash  # noqa: E402
from src.services.report import submit  # noqa: E402
from src.schemas.requests import RunInput,ScheduleInput  # noqa: E402
from src.services.delivery import add_schedule  # noqa: E402

POL={'version':'工时口径-v1','weights':{'光学研究':37,'材料研究':41,'工艺研究':22}}
SRC=[{'period':'2026-11','department':'公共','category':'仪器维护','amount_cents':150001},
     {'period':'2026-11','department':'光学研究','category':'试验耗材','amount_cents':99}]

def seed():
 with Session() as s:
  if not s.query(User).first():
   s.add(User(email='admin@demo.local',role='admin',password=password_hash('Demo2026!')))
   s.add(Policy(version=POL['version'],data=POL))
   for i,d in enumerate(SRC): s.add(Source(natural_key=f'k{i}',data=d))
   s.commit()

def admin_user():
 with Session() as s: return s.query(User).filter_by(email='admin@demo.local').first()

def queue(name):
 with Session() as s:
  return submit(s,admin_user(),RunInput(name=name,policy_id=1,period='2026-11')).id

def state(run_id):
 with Session() as s:
  j=s.get(Run,run_id);return j.status,j.worker_id,j.attempts,j.result is not None,j.error

def fault(point,on):
 with Session() as s:
  r=s.query(FaultRule).filter_by(point=point).first()
  if not r: r=FaultRule(point=point,enabled=False);s.add(r)
  r.enabled=on;s.commit()

def start_worker(wid):
 env=dict(ENV,WORKER_ID=wid)
 logf=open(f'/tmp/worker-{wid}-{time.time_ns()}.log','w')
 p=subprocess.Popen([sys.executable,'-c',
   'import src.worker,time\n'
   'while True:\n'
   ' src.worker.Path("/tmp/worker-heartbeat").write_text(str(time.time()))\n'
   ' try: src.worker.run_once()\n'
   ' except Exception: pass\n'
   ' time.sleep(0.3)'],env=env,cwd='/workspace/backend',
   stdout=logf,stderr=subprocess.STDOUT,text=True)
 return p

def wait_until(pred,timeout=25):
 t=time.time()
 while time.time()-t<timeout:
  try:
   if pred(): return True
  except Exception: pass
  time.sleep(0.3)
 return False

seed()

print('=== 场景① claim 取走任务后崩溃，租约过期被接管 ===')
id1=queue('接管-取任务崩');fault('claim',True)
a=start_worker('worker-A')
assert wait_until(lambda: a.poll()==88),'A 应在取任务后以 88 退出'
print('  worker-A 退出码',a.poll(),'任务 =',state(id1)[:3])
assert state(id1)[0]=='running' and state(id1)[1]=='worker-A'
b=start_worker('worker-B')
assert wait_until(lambda: state(id1)[0]=='completed' and state(id1)[1]=='worker-B')
print('  接管后任务 =',state(id1));assert state(id1)[2]==2
b.terminate()

print('=== 场景② result 保存结果前崩溃，接管者重算（同一冻结底稿）===')
id2=queue('接管-存结果崩');fault('result',True)
c=start_worker('worker-C')
assert wait_until(lambda: c.poll()==88),'C 应在存结果时退出'
print('  worker-C 退出码',c.poll(),'任务 =',state(id2))
assert state(id2)[0]=='running' and state(id2)[3] is False
with Session() as s: checksum=s.get(Run,id2).checksum;snapshot=s.get(Run,id2).snapshot
d=start_worker('worker-D')
assert wait_until(lambda: state(id2)[0]=='completed' and state(id2)[1]=='worker-D')
print('  重算后任务 =',state(id2))
with Session() as s:
 j=s.get(Run,id2)
 assert j.checksum==checksum and j.snapshot==snapshot and j.result['summary']['守恒差额分']==0
d.terminate()

print('=== 场景③ deliver 写成交付前崩溃，outbox 重放且只一份 ===')
with Session() as s:
 j=s.get(Run,id1);j.approved=True;j.review='端到端核对无误，同意交付'
 sch=add_schedule(s,admin_user(),ScheduleInput(name='月结交付',run_id=id1,interval_seconds=5));s.commit()
fault('deliver',True)
e=start_worker('worker-E')
assert wait_until(lambda: e.poll()==88),'E 应在写交付时退出'
with Session() as s:
 pend=s.query(DeliveryOutbox).filter_by(status='pending').count();dn=s.query(Delivery).count()
print('  崩溃后 outbox pending =',pend,' deliveries =',dn);assert pend>=1 and dn==0
f=start_worker('worker-F');time.sleep(7);f.terminate()
with Session() as s:
 dn=s.query(Delivery).count();left=s.query(DeliveryOutbox).filter_by(status='pending').count()
 rows=s.query(Delivery).all()
 # 每个到期点（due_key）恰好一份交付：无重复；事件全部投递成功
 assert len(rows)==len({x.due_key for x in rows})==s.query(DeliveryOutbox).count()
 assert all(x.run_id==id1 for x in rows)  # 固定引用原报告
 keys=sorted(x.due_key for x in rows)
 ev=[(x.due_key,x.status,x.attempts) for x in s.query(DeliveryOutbox).all()]
print('  重放后 deliveries =',dn,' pending =',left,' 事件 =',ev)
assert left==0 and len(keys)>=1
print('  各到期点唯一交付键 =',keys)

print('=== 三个故障规则均一次性失效 ===')
with Session() as s:
 rules={r.point:(r.enabled,r.fires) for r in s.query(FaultRule).all()}
print(' ',rules)
assert all(not v[0] and v[1]==1 for v in rules.values())
print('\n全部多进程崩溃-接管-重放断言通过 ✔')
