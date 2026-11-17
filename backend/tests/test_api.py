import pytest
from fastapi.testclient import TestClient
from src.main import app
from src.config.database import Session
from src.models.run import FaultRule,DeliveryOutbox,Delivery
from conftest import make_completed_approved,make_schedule,make_run

client=TestClient(app)

def _login(role):
 r=client.post('/auth/login',json={'email':f'{role}@demo.local','password':'Demo2026!'})
 assert r.status_code==200
 return {'Authorization':'Bearer '+r.json()['token']}

def test_fault_rules_admin_only_and_one_shot(db):
 admin=_login('admin');viewer=_login('viewer');analyst=_login('analyst')
 assert client.get('/faults',headers=viewer).status_code==403
 assert client.post('/faults',headers=analyst,json={'point':'claim','enabled':True}).status_code==403
 r=client.get('/faults',headers=admin);assert r.status_code==200
 assert r.json()['points']==['claim','result','deliver']
 r=client.post('/faults',headers=admin,json={'point':'result','enabled':True,'note':'演示崩溃'})
 assert r.status_code==200 and r.json()['enabled'] is True
 r=client.post('/faults',headers=admin,json={'point':'unknown','enabled':True})
 assert r.status_code==422
 r=client.post('/faults/reset',headers=admin);assert r.status_code==200
 with Session() as s: assert all(not x.enabled for x in s.query(FaultRule).all())

def test_delivery_events_endpoint_tracks_recovery(db,clock):
 admin=_login('admin')
 run=make_completed_approved(db);make_schedule(db,run)
 clock.advance(61)
 from src.services import delivery
 delivery.sweep_schedules(db,'worker-A')
 r=client.get('/delivery-events',headers=admin)
 assert r.status_code==200 and len(r.json())==1
 event=r.json()[0]
 assert event['status']=='pending' and event['run_id']==run.id
 delivery.tick(db,'worker-A')
 r=client.get('/delivery-events',headers=admin)
 assert r.json()[0]['status']=='sent' and r.json()[0]['attempts']>=1

def test_requires_auth(db):
 assert client.get('/runs').status_code==401
 assert client.get('/delivery-events').status_code==401

def test_full_queue_to_delivery_flow_via_api(db,clock):
 """端到端：API 排队 → worker 补领完成 → 复核 → 计划 → 交付，全程数据库权威。"""
 admin=_login('admin');analyst=_login('analyst')
 r=client.post('/runs',headers=analyst,json={'name':'端到端核算','policy_id':1,'period':'2026-11'})
 assert r.status_code==200;run_id=r.json()['id']
 from src.worker import run_once
 run_once('worker-A')
 r=client.get(f'/runs/{run_id}',headers=admin);assert r.json()['status']=='completed'
 r=client.post(f'/runs/{run_id}/approve',headers=admin,json={'note':'数值核对无误，同意复核'})
 assert r.status_code==200 and r.json()['approved'] is True
 r=client.post('/schedules',headers=admin,json={'name':'月结','run_id':run_id,'interval_seconds':5})
 assert r.status_code==200
 clock.advance(6)
 run_once('worker-A')
 r=client.get('/deliveries',headers=admin)
 assert len(r.json())==1 and r.json()[0]['run_id']==run_id
 r=client.get(f"/deliveries/{r.json()[0]['id']}/csv",headers=admin)
 assert r.status_code==200 and 'text/csv' in r.headers['content-type']
