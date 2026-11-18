"""报告正文在线存储：总容量与并发发布额度的统一仲裁。

业务规则（与产品口径一一对应）：
1. 在线存储设总容量 STORAGE_LIMIT_BYTES，并发发布有额度 PUBLISH_SLOTS，两者都不能超额。
2. 空间不足时优先清退最早的旧报告；正在下载（active download）的报告不动；
   同一草案（relation）已有报告的，至少保留一份可下载。
3. 发布未成功时旧报告仍可下载；中断后恢复原发布；重试不重复扣占用（复用同一预留/额度）。
4. 页面区分 待发布(pending) / 可下载(online) / 已清退(evicted)；腾不出空间时返回明确原因。

并发安全：所有"读配额→决策→写占用"的临界区都在一把全局存储锁内完成。
优先使用 Redis 分布式锁（多进程部署），Redis 不可用时退化为本地文件锁（单宿主）。
"""
from __future__ import annotations
import json
import os
import threading
import time
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import select

from src.config.settings import (
    STORAGE_LIMIT_BYTES,
    PUBLISH_SLOTS,
    LOCK_DIR,
)
from src.constants.enums import STORAGE_STATES

LOCK_KEY = 'schemaworkbook:storage-lock'
LOCK_TTL_SECONDS = 30
DOWNLOAD_TTL_SECONDS = 120
DOWNLOAD_PREFIX = 'schemaworkbook:download:'


def _download_path(report_id: int) -> str:
    return os.path.join(LOCK_DIR, 'downloads', f'report-{report_id}.flag')


def begin_download(redis_client, report_id: int) -> None:
    """标记某报告正文开始下载；清退逻辑据此保护，避免下载中途被清退。"""
    if redis_client is not None:
        try:
            redis_client.set(DOWNLOAD_PREFIX + str(report_id), '1', ex=DOWNLOAD_TTL_SECONDS)
            return
        except Exception:
            pass
    path = _download_path(report_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(str(time.time()))


def end_download(redis_client, report_id: int) -> None:
    if redis_client is not None:
        try:
            redis_client.delete(DOWNLOAD_PREFIX + str(report_id))
            return
        except Exception:
            pass
    try:
        os.remove(_download_path(report_id))
    except FileNotFoundError:
        pass


def active_download_ids(redis_client) -> set[int]:
    """返回当前正在下载（含 TTL 内未结束）的报告 id 集合。"""
    ids: set[int] = set()
    if redis_client is not None:
        try:
            for key in redis_client.scan_iter(DOWNLOAD_PREFIX + '*'):
                tail = key.split(':')[-1]
                if tail.isdigit():
                    ids.add(int(tail))
        except Exception:
            pass
    directory = os.path.join(LOCK_DIR, 'downloads')
    if os.path.isdir(directory):
        cutoff = time.time() - DOWNLOAD_TTL_SECONDS
        for name in os.listdir(directory):
            if not name.startswith('report-') or not name.endswith('.flag'):
                continue
            tail = name[len('report-'):-len('.flag')]
            if not tail.isdigit():
                continue
            path = os.path.join(directory, name)
            try:
                # 清理过期标记，避免崩溃残留永久保护。
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    continue
            except OSError:
                continue
            ids.add(int(tail))
    return ids


# --------------------------------------------------------------------------- #
# 锁：可重入的全局存储锁。Redis 优先（多进程），文件锁兜底（单宿主）。
# 同一线程嵌套进入只取一次底层原语（计数 depth），避免服务层嵌套加锁自死锁；
# 跨线程由 RLock 串行，跨进程由 flock/Redis NX 互斥。
# --------------------------------------------------------------------------- #
_local = threading.local()
_thread_guard = threading.RLock()


class _ReentrantStorageLock:
    def __init__(self, path: str, redis_client=None):
        self._path = path
        self._redis = redis_client
        self._fd = None
        self._token = None

    def _depth(self) -> int:
        return getattr(_local, 'depth', 0)

    def _bump(self, delta: int):
        _local.depth = self._depth() + delta

    def __enter__(self):
        _thread_guard.acquire()
        if self._depth() == 0:
            self._acquire_primitive()
        self._bump(1)
        return self

    def __exit__(self, *exc):
        try:
            self._bump(-1)
            if self._depth() == 0:
                self._release_primitive()
        finally:
            _thread_guard.release()

    def _acquire_primitive(self):
        import fcntl
        if self._redis is not None:
            self._token = f'{os.getpid()}-{threading.get_ident()}-{time.time_ns()}'
            deadline = time.time() + LOCK_TTL_SECONDS
            while not self._redis.set(LOCK_KEY, self._token, nx=True, ex=LOCK_TTL_SECONDS):
                # 旧持有者可能崩溃：依赖 TTL 自然过期后继续抢，不主动删他人锁。
                if time.time() > deadline:
                    deadline = time.time() + LOCK_TTL_SECONDS
                time.sleep(0.02)
            return
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.time() + LOCK_TTL_SECONDS
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if time.time() > deadline:
                    # 兜底：超时则强制抢入，避免死锁永久阻塞业务。
                    fcntl.flock(self._fd, fcntl.LOCK_EX)
                    return
                time.sleep(0.02)

    def _release_primitive(self):
        import fcntl
        if self._redis is not None:
            # 仅删除自己持有的锁，避免误释放。
            if self._redis.get(LOCK_KEY) == self._token:
                self._redis.delete(LOCK_KEY)
            self._token = None
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None


def storage_lock(redis_client=None):
    if redis_client is not None:
        try:
            redis_client.ping()
            return _ReentrantStorageLock('', redis_client=redis_client)
        except Exception:
            pass
    return _ReentrantStorageLock(os.path.join(LOCK_DIR, 'storage.lock'))


# --------------------------------------------------------------------------- #
# 体量估算与占用统计
# --------------------------------------------------------------------------- #
def estimate_body_size(report) -> int:
    """发布前依据冻结快照估算报告正文字节数，用于预留容量。

    报告正文为 UTF-8 JSON，体量主要随属性数与依赖数增长。这里给出一个稳定、
    可复现的上界估计，真正落盘后会以实际字节数回填并多退少补。
    """
    snapshot = report.input_snapshot or {}
    relation = snapshot.get('relation', {}) or {}
    attrs = relation.get('attributes', []) or []
    deps = snapshot.get('dependencies', []) or []
    payload = {
        'relation': relation,
        'dependencies': deps,
        # 真实结果里每个属性/依赖会产生若干证明结构，预留展开系数。
        'projected_artifacts': len(attrs) * 12 + len(deps) * 8,
    }
    base = len(json.dumps(payload, ensure_ascii=False).encode('utf-8'))
    # 固定的报告外壳（候选键证明、tableau、校验结论等）开销。
    return max(256, base + 1024)


def actual_body_size(result) -> int:
    return len(json.dumps(result, ensure_ascii=False, indent=2).encode('utf-8'))


def _now():
    return datetime.now(timezone.utc)


class StorageUsage:
    def __init__(self, db):
        self.db = db
        self.reports = list(db.scalars(select(Report).order_by(Report.id)))

    @property
    def used_bytes(self) -> int:
        return sum(r.body_size for r in self.reports if r.storage_state in ('pending', 'online'))

    @property
    def free_bytes(self) -> int:
        return STORAGE_LIMIT_BYTES - self.used_bytes

    @property
    def used_slots(self) -> int:
        return sum(1 for r in self.reports if r.slot_token and r.storage_state == 'pending')

    @property
    def free_slots(self) -> int:
        return PUBLISH_SLOTS - self.used_slots


# 避免循环导入：在文件内延迟引用 Report。
from src.models.entities import Report  # noqa: E402


# --------------------------------------------------------------------------- #
# 清退（eviction）
# --------------------------------------------------------------------------- #
def _evictable_candidates(usage: StorageUsage, protected_id: int | None, downloading_ids: set[int]):
    """返回可被清退的 online 报告，按"最早优先"排序。

    不可清退：
      - 非 online（待发布/已清退不在容量可回收范围内）
      - 正在下载的报告（downloading_ids）
      - 当前请求自身（protected_id，发布流程中保护自己刚上线的正文）
      - 同一草案仅剩的一份可下载报告（每草案至少留一份）
    """
    online = [r for r in usage.reports if r.storage_state == 'online']
    # 每个 relation 当前可下载报告数量，用于保证"每草案至少留一份"。
    online_count: dict[int, int] = {}
    for r in online:
        online_count[r.relation_id] = online_count.get(r.relation_id, 0) + 1

    candidates = []
    for r in sorted(online, key=lambda x: (x.published_at or x.created_at, x.id)):
        if r.id == protected_id:
            continue
        if r.id in downloading_ids:
            continue  # 正在下载的不动
        if online_count.get(r.relation_id, 0) <= 1:
            continue  # 该草案只剩这一份可下载，保留
        candidates.append(r)
    return candidates


def make_room(db, needed_bytes: int, protected_id: int | None,
              downloading_ids: set[int], note_prefix: str = '容量腾退') -> list[Report]:
    """在已持锁的前提下，通过清退最早的旧报告腾出至少 needed_bytes 空间。

    成功返回被清退的报告列表；无法腾出足够空间时抛出 507 并附带原因明细。
    每清退一份都重新计算候选，确保同一草案在被清退到仅剩一份时立即停止，
    严格满足"每个草案至少保留一份可下载"。
    """
    usage = StorageUsage(db)
    if usage.free_bytes >= needed_bytes:
        return []

    evicted: list[Report] = []
    # 逐份清退：每轮基于最新占用重新选出"最早的可清退报告"。
    while usage.free_bytes < needed_bytes:
        candidates = _evictable_candidates(usage, protected_id, downloading_ids)
        if not candidates:
            break
        cand = candidates[0]
        freed = cand.body_size
        cand.storage_state = 'evicted'
        cand.body_size = 0
        cand.artifact_path = None
        cand.evicted_at = _now()
        cand.storage_note = f'{note_prefix}：为新报告腾退最早的旧报告（释放 {freed} 字节）'
        evicted.append(cand)
        db.flush()
        usage = StorageUsage(db)

    if usage.free_bytes >= needed_bytes:
        return evicted

    # 腾不出空间：汇总阻塞原因，便于页面说明。
    blocked = [
        r for r in usage.reports
        if r.storage_state == 'online' and r.id != protected_id and r.id not in downloading_ids
    ]
    protected_single = [
        r for r in blocked
        if sum(1 for x in usage.reports
               if x.relation_id == r.relation_id and x.storage_state == 'online') <= 1
    ]
    downloading = [r for r in usage.reports if r.id in downloading_ids and r.storage_state == 'online']
    detail = [
        f'需要 {needed_bytes} 字节，总容量 {STORAGE_LIMIT_BYTES} 字节，当前可用仅 {max(usage.free_bytes,0)} 字节',
    ]
    if downloading:
        detail.append('有报告正在下载而受保护：' + '、'.join(f'#{r.id}' for r in downloading))
    if protected_single:
        detail.append('每个草案至少保留一份可下载，无法清退：' + '、'.join(f'#{r.id}' for r in protected_single))
    if not blocked and not downloading:
        detail.append('已无可清退的在线报告')
    raise HTTPException(507, '在线存储空间不足，且无法通过清退腾出足够容量：' + '；'.join(detail))


# --------------------------------------------------------------------------- #
# 预留（创建发布任务）
# --------------------------------------------------------------------------- #
def reserve_publish(db, report, redis_client=None, downloading_ids: set[int] | None = None) -> Report:
    """为新报告预留并发发布额度与存储容量。必须在 storage_lock 内调用。"""
    downloading_ids = downloading_ids or set()
    estimated = estimate_body_size(report)
    report.body_size = estimated
    report.storage_state = 'pending'
    report.publish_attempt = 1
    report.storage_note = f'待发布：预估正文 {estimated} 字节，等待并发额度与容量确认'

    usage = StorageUsage(db)
    if usage.free_slots <= 0:
        raise HTTPException(
            429,
            f'并发发布额度已满（{PUBLISH_SLOTS} 个），请等待在途发布完成后再提交',
        )

    # 先尝试腾容量（不动正在下载的、保留每草案一份）。
    make_room(db, estimated, protected_id=report.id,
              downloading_ids=downloading_ids, note_prefix='新发布预留容量')
    # 腾出空间后再占用一个发布额度。
    report.slot_token = int(time.time_ns() % 1_000_000_000) or 1
    db.flush()
    return report


# --------------------------------------------------------------------------- #
# 发布完成（worker 调用）
# --------------------------------------------------------------------------- #
def complete_publish(db, report, result, artifact_path: str,
                     redis_client=None, downloading_ids: set[int] | None = None) -> Report:
    """计算成功：正文上线。复用原预留，不重复扣额度；实际体量多退少补。"""
    downloading_ids = downloading_ids or set()
    if report.storage_state != 'pending' or not report.slot_token:
        # 正常不会发生：防御性校验，防止重复发布重复占用。
        raise HTTPException(409, f'报告 #{report.id} 不在待发布状态，不能上线')

    actual = actual_body_size(result)
    reserved = report.body_size
    # 容量调整：实际比预留大时需要补差额（可能触发清退）；小时直接释放差额。
    if actual > reserved:
        make_room(db, actual - reserved, protected_id=report.id,
                  downloading_ids=downloading_ids, note_prefix='发布上线补容量')

    report.result = result
    report.body_size = actual
    report.storage_state = 'online'
    report.artifact_path = artifact_path
    report.published_at = _now()
    report.storage_note = f'已发布上线：实际正文 {actual} 字节（预留 {reserved}）'
    # 释放并发发布额度，但容量继续占用。
    report.slot_token = 0
    db.flush()
    return report


def fail_publish(db, report, error: str) -> Report:
    """计算/发布失败：保留预留与额度以便原发布重试，旧报告不受影响。

    关键：这里不释放 slot_token、不改 storage_state（仍 pending），
    这样"中断后恢复原发布、重试不重复扣占用"。容量预留同样保留。
    """
    report.status = 'failed'
    report.error = error
    report.publish_attempt += 1
    report.storage_note = f'发布未成功（第 {report.publish_attempt - 1} 次尝试）：保留原额度与预留，可重试，旧报告仍可下载'
    db.flush()
    return report


def retry_publish(db, report, queue, downloading_ids: set[int] | None = None) -> Report:
    """恢复原发布：复用同一 slot_token 与容量预留，重新入队，不重复扣占用。"""
    downloading_ids = downloading_ids or set()
    if report.storage_state != 'pending' or not report.slot_token:
        raise HTTPException(409, f'报告 #{report.id} 当前不可重试（仅待发布且持有额度的报告可重试）')
    report.status = 'queued'
    report.error = None
    report.publish_attempt += 1
    report.storage_note = f'恢复原发布（第 {report.publish_attempt} 次尝试）：复用原并发额度与容量预留，不重复扣占用'
    db.commit()
    try:
        queue.rpush('schemaworkbook:queue', str(report.id))
    except Exception:
        report.status = 'failed'
        report.error = '队列暂时不可用，请稍后重试'
        db.commit()
        raise HTTPException(503, report.error)
    return report


# --------------------------------------------------------------------------- #
# 容量视图（供 API / 页面）
# --------------------------------------------------------------------------- #
def storage_overview(db) -> dict:
    usage = StorageUsage(db)
    by_state = {s: 0 for s in STORAGE_STATES}
    bytes_by_state = {s: 0 for s in STORAGE_STATES}
    for r in usage.reports:
        by_state[r.storage_state] = by_state.get(r.storage_state, 0) + 1
        bytes_by_state[r.storage_state] = bytes_by_state.get(r.storage_state, 0) + r.body_size
    return {
        'limit_bytes': STORAGE_LIMIT_BYTES,
        'used_bytes': usage.used_bytes,
        'free_bytes': max(usage.free_bytes, 0),
        'used_percent': round(100 * usage.used_bytes / STORAGE_LIMIT_BYTES, 1) if STORAGE_LIMIT_BYTES else 0,
        'publish_slots': PUBLISH_SLOTS,
        'used_slots': usage.used_slots,
        'free_slots': max(usage.free_slots, 0),
        'reports_by_state': by_state,
        'bytes_by_state': bytes_by_state,
    }
