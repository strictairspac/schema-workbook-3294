from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import Relation, Dependency, Report, Audit
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from . import storage

queue = Redis.from_url(REDIS_URL, decode_responses=True)


def _client():
    """返回可用的 Redis 客户端；不可用时返回 None，存储服务自动退化为文件锁。"""
    try:
        queue.ping()
        return queue
    except Exception:
        return None


def listing(db):
    return [record(x) for x in db.scalars(select(Report).order_by(Report.id.desc()))]


def detail(db, ident):
    obj = db.get(Report, ident)
    if not obj:
        raise HTTPException(404, '报告不存在')
    return record(obj)


def create(db, data, user):
    relation = db.get(Relation, data.relation_id)
    if not relation:
        raise HTTPException(404, '关系不存在')
    deps = list(db.scalars(select(Dependency).where(Dependency.relation_id == relation.id)))
    snapshot = {
        'relation': record(relation),
        'dependencies': [{'lhs': x.lhs, 'rhs': x.rhs} for x in deps],
    }
    obj = Report(**data.model_dump(), input_snapshot=snapshot, status=RUN_STATES[0])

    client = _client()
    # 临界区：在全局存储锁内完成"读配额→清退→预留→落库"，并发发布不会超额。
    with storage.storage_lock(client):
        storage.reserve_publish(
            db, obj, redis_client=client,
            downloading_ids=storage.active_download_ids(client),
        )
        db.add(obj)
        db.add(Audit(actor=user.email, action='范式分解入队 ' + data.name))
        try:
            db.commit()
        except Exception:
            db.rollback()
            raise
        report_id = obj.id
        # 入队失败：回滚预留（释放额度与容量），避免悬挂占用。
        push_client = client if client is not None else queue
        try:
            push_client.rpush('schemaworkbook:queue', str(report_id))
        except Exception:
            with storage.storage_lock(client):
                fresh = db.get(Report, report_id)
                if fresh:
                    fresh.status = 'failed'
                    fresh.slot_token = 0
                    fresh.storage_state = 'evicted'
                    fresh.body_size = 0
                    fresh.storage_note = '队列暂时不可用，已释放预留，请重新创建'
                    fresh.error = '队列暂时不可用，请重新创建'
                    db.commit()
            raise HTTPException(503, '队列暂时不可用，请重新创建')

    return record(db.get(Report, report_id))


def retry(db, ident, user):
    """中断后恢复原发布：复用原额度与预留，重新入队，不重复扣占用。"""
    obj = db.get(Report, ident)
    if not obj:
        raise HTTPException(404, '报告不存在')
    client = _client()
    with storage.storage_lock(client):
        # 重新绑定到当前会话，确保锁内读到最新状态。
        obj = db.get(Report, ident)
        return record(storage.retry_publish(
            db, obj, client if client is not None else queue,
            downloading_ids=storage.active_download_ids(client),
        ))


def overview(db):
    return storage.storage_overview(db)
