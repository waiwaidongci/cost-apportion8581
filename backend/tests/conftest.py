import os,pathlib,types
DB_PATH='/tmp/cost-reliability.db'
pathlib.Path(DB_PATH).unlink(missing_ok=True)
os.environ['DATABASE_URL']=f'sqlite:///{DB_PATH}'
os.environ['JWT_SECRET']='test-secret'
os.environ['PROJECT_JSON']='/workspace/backend/project.json'
os.environ['REDIS_URL']='redis://fake/0'
os.environ['WORKER_LEASE_SECONDS']='15'

import pytest,fakeredis,redis as redis_pkg
from src.config.database import Base,engine,Session
from src.config.migrate import upgrade
from src.models.identity import User
from src.models.source import Source,Policy
from src.models.run import Run,FaultRule
from src.utils.security import password_hash
from src.services import faults
from src.services.calculation import calculate

upgrade()

POLICY={'version':'工时口径-v1','weights':{'光学研究':37,'材料研究':41,'工艺研究':22}}
SOURCES=[
 {'period':'2026-11','department':'公共','category':'仪器维护','amount_cents':150001},
 {'period':'2026-11','department':'光学研究','category':'试验耗材','amount_cents':99},
]

class Clock:
 def __init__(self,t=1000.0): self.t=t
 def time(self): return self.t
 def advance(self,dt): self.t+=dt

@pytest.fixture
def clock(monkeypatch):
 c=Clock()
 import src.services.tasks as tasks
 import src.services.delivery as delivery
 import src.services.report as report
 import src.services.faults as faults
 import src.services.audit as audit
 for mod in (tasks,delivery,report,faults,audit):
  monkeypatch.setattr(mod.time,'time',c.time)
 return c

@pytest.fixture
def db():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
 with Session() as s:
  s.add(User(email='admin@demo.local',role='admin',password=password_hash('Demo2026!')))
  s.add(User(email='analyst@demo.local',role='analyst',password=password_hash('Demo2026!')))
  s.add(User(email='viewer@demo.local',role='viewer',password=password_hash('Demo2026!')))
  s.add(Policy(version=POLICY['version'],data=POLICY))
  for i,data in enumerate(SOURCES): s.add(Source(natural_key=f'k{i+1}',data=data))
  s.commit()
  with Session() as inner:
   yield inner

@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):
 fr=fakeredis.FakeRedis(decode_responses=False)
 fake_mod=types.SimpleNamespace(Redis=type('FakeRedisCls',(),{'from_url':staticmethod(lambda *a,**k:fr)}))
 monkeypatch.setattr(redis_pkg,'Redis',fake_mod.Redis)
 import src.worker as worker
 monkeypatch.setattr(worker,'r',fr)
 return fr

@pytest.fixture
def crash(monkeypatch):
 def boom(point): raise faults.InjectedFault(point)
 monkeypatch.setattr(faults,'terminate',boom)

def make_run(db,name='十一月核算',period='2026-11'):
 from src.schemas.requests import RunInput
 from src.services.report import submit
 user=db.query(User).filter_by(email='analyst@demo.local').first()
 return submit(db,user,RunInput(name=name,policy_id=1,period=period))

def make_completed_approved(db):
 run=make_run(db)
 run.result=calculate(run.snapshot['sources'],run.snapshot['policy'])
 run.status='completed';run.approved=True;run.review='核对无误，口径与数值一致';db.commit()
 return run

def make_schedule(db,run):
 from src.schemas.requests import ScheduleInput
 from src.services.delivery import add_schedule
 admin=db.query(User).filter_by(email='admin@demo.local').first()
 return add_schedule(db,admin,ScheduleInput(name='月结交付',run_id=run.id,interval_seconds=60))
