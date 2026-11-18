from sqlalchemy import Column,Integer,String,JSON,DateTime,ForeignKey,Text
from datetime import datetime,timezone
from src.config.database import Base
class User(Base):
    __tablename__='users'
    id=Column(Integer,primary_key=True);email=Column(String(120),unique=True,nullable=False);password=Column(String(300),nullable=False);role=Column(String(20),nullable=False)
class Relation(Base):
    __tablename__='relations'
    id=Column(Integer,primary_key=True);code=Column(String(80),unique=True,nullable=False);title=Column(String(160),nullable=False);attributes=Column(JSON,nullable=False);note=Column(String(300),default='')
class Dependency(Base):
    __tablename__='dependencies'
    id=Column(Integer,primary_key=True);relation_id=Column(Integer,ForeignKey('relations.id'),nullable=False);lhs=Column(JSON,nullable=False);rhs=Column(JSON,nullable=False);note=Column(String(300),default='')
class Report(Base):
    __tablename__='reports'
    id=Column(Integer,primary_key=True);name=Column(String(120),nullable=False);relation_id=Column(Integer,ForeignKey('relations.id'),nullable=False)
    status=Column(String(20),nullable=False,default='queued');input_snapshot=Column(JSON,nullable=False);result=Column(JSON,nullable=True);error=Column(Text,nullable=True)
    # —— 报告正文在线存储与并发发布配额 ——
    # storage_state: pending(待发布) / online(可下载) / evicted(已清退)
    storage_state=Column(String(20),nullable=False,default='pending')
    # 报告正文字节数：发布前为预估值（预留容量），发布后为实际值；清退后归零
    body_size=Column(Integer,nullable=False,default=0)
    # 发布尝试序号：同一报告重试复用同一额度/预留，不重复扣占用
    publish_attempt=Column(Integer,nullable=False,default=0)
    # 占用一个并发发布额度（slot）；释放后置 0
    slot_token=Column(Integer,nullable=False,default=0)
    # 最近一次存储决策的说明（清退原因 / 容量不足原因 / 发布结果），供页面展示
    storage_note=Column(String(500),nullable=False,default='')
    # 正文实际落盘路径（online 时有值）
    artifact_path=Column(String(300),nullable=True)
    created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
    published_at=Column(DateTime(timezone=True),nullable=True)
    evicted_at=Column(DateTime(timezone=True),nullable=True)
class Audit(Base):
    __tablename__='audit_logs'
    id=Column(Integer,primary_key=True);actor=Column(String(120),nullable=False);action=Column(String(160),nullable=False);created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
