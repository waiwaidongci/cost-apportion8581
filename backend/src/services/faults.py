"""故障注入：规则存数据库，对多执行者均可见；一次性触发，触发后自动失效。

三个注入点对应任务生命周期的关键写时点：
- claim   取走任务：任务已被该执行者条件更新领取，提交后立即崩溃，租约等待接管
- result  保存结果：计算完成、结果写入前崩溃，接管者重新计算（迟到结果无权覆盖）
- deliver 写成交付：待交付事件已落盘、写 deliveries 前崩溃，重放后仍只产生一份
"""
import os,time,logging
from src.models.run import FaultRule
from src.services.audit import log

POINTS=('claim','result','deliver')
DEFAULT_LEASE_SECONDS=30.0

def lease_seconds():
 try: return max(3.0,float(os.getenv('WORKER_LEASE_SECONDS',DEFAULT_LEASE_SECONDS)))
 except ValueError: return DEFAULT_LEASE_SECONDS

def worker_identity():
 return os.getenv('WORKER_ID') or f"{os.uname().nodename}-{os.getpid()}"

def set_rule(db,point,enabled,note='',actor='admin'):
 if point not in POINTS: raise ValueError('未知故障点')
 obj=db.query(FaultRule).filter_by(point=point).first()
 if not obj:
  obj=FaultRule(point=point,enabled=False,note=note);db.add(obj)
 obj.enabled=bool(enabled)
 if note: obj.note=note
 log(db,actor,f'fault.{point}.{"on" if enabled else "off"}',note or point)
 db.commit();return obj

def list_rules(db):
 rules={r.point:r for r in db.query(FaultRule).all()}
 return [rules.get(p) or FaultRule(point=p,enabled=False) for p in POINTS]

class InjectedFault(RuntimeError):
 def __init__(self,point):
  super().__init__(f'故障注入：{point} 时点进程退出');self.point=point

def terminate(point):
 """真实环境立即退出进程（不做任何清理，模拟崩溃）；测试可替换为抛异常。"""
 os._exit(88)

def trigger(db,point,context):
 """在工作进程的事务边界处调用；命中则先把规则置为失效（保证一次性）并落审计，再退出进程。"""
 rule=db.query(FaultRule).filter_by(point=point,enabled=True).first()
 if not rule: return False
 rule.enabled=False;rule.fires=(rule.fires or 0)+1;rule.last_at=time.time()
 log(db,context.get('worker','worker'),f'fault.{point}.fired',str(context.get('target','')))
 db.commit()
 logging.getLogger('worker').error('故障注入命中 point=%s target=%s，进程退出，等待租约接管/重放',point,context.get('target'))
 terminate(point)
 return True
