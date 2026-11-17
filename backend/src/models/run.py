from sqlalchemy import Column,Integer,String,JSON,Text,Float,Boolean,Index
from src.config.database import Base
class Run(Base):
 __tablename__='allocationruns'
 id=Column(Integer,primary_key=True); name=Column(String(120)); status=Column(String(20)); snapshot=Column(JSON); checksum=Column(String(64)); result=Column(JSON); error=Column(Text); created=Column(Float); approved=Column(Boolean,default=False); review=Column(Text,default='')
 # 租约接管：以数据库任务行为权威，Redis 队列丢失/清空也可补领
 attempts=Column(Integer,default=0,nullable=False)          # 累计领取次数（含接管）
 worker_id=Column(String(120))                              # 当前持有者
 lease_expires_at=Column(Float)                             # 租约到期时间戳
 heartbeat_at=Column(Float)                                 # 最近一次心跳
 __table_args__=(Index('ix_runs_claim','status','lease_expires_at'),)
class Schedule(Base):
 __tablename__='delivery_schedules'
 id=Column(Integer,primary_key=True); name=Column(String(120)); run_id=Column(Integer); interval_seconds=Column(Integer); next_at=Column(Float); enabled=Column(Boolean,default=True)
class Delivery(Base):
 __tablename__='deliveries'
 id=Column(Integer,primary_key=True); schedule_id=Column(Integer); run_id=Column(Integer); due_key=Column(String(120),unique=True); created=Column(Float); payload=Column(JSON)
class DeliveryOutbox(Base):
 """待交付事件：与计划推进同事务落盘，崩溃后重放，delivery 的 due_key 唯一约束保证幂等不重复"""
 __tablename__='delivery_outbox'
 id=Column(Integer,primary_key=True); schedule_id=Column(Integer,index=True); run_id=Column(Integer); due_key=Column(String(120),unique=True); due_at=Column(Float); status=Column(String(20),default='pending',index=True); attempts=Column(Integer,default=0); last_error=Column(Text); created=Column(Float); sent_at=Column(Float)
class FaultRule(Base):
 """故障注入开关（一次性，触发后自动失效），覆盖取走任务/保存结果/写成交付三个时点"""
 __tablename__='fault_rules'
 id=Column(Integer,primary_key=True); point=Column(String(40),unique=True); enabled=Column(Boolean,default=False); fires=Column(Integer,default=0); last_at=Column(Float); note=Column(String(250))
