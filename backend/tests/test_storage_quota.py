"""报告正文在线存储配额：端到端行为验证（SQLite + 文件锁，无外部依赖）。

运行（在 backend 目录下）：
    python3 -m tests.test_storage_quota

覆盖需求：
  T1 总容量与并发发布额度都不能超额
  T2 空间不足优先清退最早旧报告；每草案至少留一份可下载；腾不出空间说明原因
  T3 下载期间腾空间：正在下载的报告不动（含 HTTP 导出 410/409 行为）
  T4 发布未成功旧报告仍可下载；中断后恢复原发布；重试不重复扣占用
  T5 多线程抢同一额度：成功数恰为额度数，容量与槽位绝不超额
  T6 实际体量大于预留时补差额并触发清退，多退少补
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading

# —— 必须在导入应用代码之前配置环境 ——
_TMP = tempfile.mkdtemp(prefix='sw-test-')
os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(_TMP, 'test.db')
os.environ['STORAGE_LOCK_DIR'] = os.path.join(_TMP, 'locks')
os.environ['STORAGE_LIMIT_BYTES'] = '1000'
os.environ['PUBLISH_SLOTS'] = '3'
os.environ['JWT_SECRET'] = 'test-secret'
# 指向不可达端口，确保 redis 客户端 ping 失败，走文件锁/本地标记降级路径。
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6399/0'
os.environ['ARTIFACT_DIR'] = os.path.join(tempfile.mkdtemp(prefix='sw-art-'), 'artifacts')

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from src.config import database as _db  # noqa: E402
from src.models.entities import User, Relation, Dependency, Report  # noqa: E402
from src.services import storage  # noqa: E402
from src.services import reports as report_service  # noqa: E402
from src.routes import reports as reports_route  # noqa: E402
from src.utils.security import writer, current  # noqa: E402
from src import worker  # noqa: E402

# 重建支持跨线程、带 busy timeout 的 SQLite 引擎（多线程抢额度测试需要）。
_engine = create_engine(
    os.environ['DATABASE_URL'],
    connect_args={'check_same_thread': False, 'timeout': 30},
    pool_pre_ping=True,
)
_db.engine = _engine
_db.Session = sessionmaker(_engine, expire_on_commit=False)
Session = _db.Session
worker.Session = _db.Session
from src.config.database import Base  # noqa: E402

PASS, FAIL = 0, 0


def check(name, condition, detail=''):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f'  ✓ {name}')
    else:
        FAIL += 1
        print(f'  ✗ {name} {detail}')


def setup_db():
    """每个用例独立建库，返回三个草案 id。"""
    Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)
    with Session() as db:
        db.add(User(email='op@nanlu.test', password='x', role='operator'))
        ids = []
        for code in ('RA', 'RB', 'RC'):
            r = Relation(code=code, title='t-' + code,
                         attributes=['委托号', '客户', '检测项'], note='')
            db.add(r)
            db.flush()
            db.add(Dependency(relation_id=r.id, lhs=['委托号'], rhs=['客户'], note=''))
            ids.append(r.id)
        db.commit()
    return ids


class _User:
    email = 'op@nanlu.test'
    role = 'operator'


def make_report(db, name, relation_id, size=200):
    """构造一条待发布报告并完成预留（固定体量，容量算术确定可预测）。"""
    rel = db.get(Relation, relation_id)
    snap = {'relation': {'id': rel.id, 'code': rel.code, 'attributes': rel.attributes},
            'dependencies': [{'lhs': ['委托号'], 'rhs': ['客户']}]}
    obj = Report(name=name, relation_id=relation_id, input_snapshot=snap,
                 status='queued', storage_state='pending')
    storage.estimate_body_size = lambda r: size  # type: ignore
    storage.reserve_publish(db, obj, redis_client=None,
                            downloading_ids=storage.active_download_ids(None))
    db.add(obj)
    db.commit()
    return db.get(Report, obj.id)


def complete(report_id, result_size):
    # 在独立会话中完成发布（模拟 worker 与 Web 请求分离）。
    with Session() as db:
        r = db.get(Report, report_id)
        result = {'checksum': 'x', 'decomposition': [], 'candidate_keys': [],
                  'closure_proofs': [], 'normal_form_checks': [], 'chase': {}}
        storage.actual_body_size = lambda res: result_size  # type: ignore
        path = os.path.join(_TMP, f'report-{report_id}.json')
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(json.dumps(result))
        storage.complete_publish(db, r, result, path, redis_client=None,
                                 downloading_ids=storage.active_download_ids(None))
        db.commit()


def refresh(db):
    """跨会话写入后，让当前会话丢弃缓存属性，强制后续读取最新已提交数据。"""
    db.expire_all()


def expect_http(status, fn):
    try:
        fn()
        return None
    except Exception as e:
        if getattr(e, 'status_code', None) == status:
            return e
        raise


def t1_capacity_and_slots():
    print('T1 总容量与并发额度不能超额')
    ra = setup_db()[0]
    with Session() as db:
        ids = [make_report(db, f'p{i}', ra, 200).id for i in range(3)]
        ov = storage.storage_overview(db)
        check('待发布占用计入容量', ov['used_bytes'] == 600, str(ov['used_bytes']))
        check('并发槽位占满', ov['used_slots'] == 3 and ov['free_slots'] == 0)
        check('容量未超额', ov['used_bytes'] <= ov['limit_bytes'])
        check('槽位满拒绝新发布(429)',
              expect_http(429, lambda: make_report(db, 'p3', ra, 200)) is not None)
        complete(ids[0], 200)
        ov = storage.storage_overview(db)
        check('上线后释放槽位但保留容量', ov['used_slots'] == 2 and ov['used_bytes'] == 600)
        check('容量不足拒绝(507)并说明原因',
              (lambda e: e is not None and '空间' in str(e))(
                  expect_http(507, lambda: make_report(db, 'big', ra, 500))))


def t2_eviction_order_and_keep_one():
    print('T2 清退最早旧报告 / 每草案留一份 / 原因说明')
    ra, rb, rc = setup_db()
    with Session() as db:
        # 随建随完成以复用并发槽位，最终得到 RA 三份 + RB 一份在线（各 200，共 800）。
        a = []
        for i in range(3):
            r = make_report(db, f'a{i}', ra, 200)
            complete(r.id, 200)
            refresh(db)
            a.append(r)
        b = make_report(db, 'b0', rb, 200)
        complete(b.id, 200)
        refresh(db)
        check('前置四份在线占 800', storage.storage_overview(db)['used_bytes'] == 800)

        # 申请 400：缺 200；RA 三份可清退最早 a0，RB 仅一份不可动。
        new = make_report(db, 'new', rc, 400)
        db.expire_all()
        check('最早旧报告被清退', db.get(Report, a[0].id).storage_state == 'evicted')
        check('清退记录保留原因', '腾退' in (db.get(Report, a[0].id).storage_note or ''))
        check('同草案较新报告保留', db.get(Report, a[1].id).storage_state == 'online')
        check('单份草案不被清退', db.get(Report, b.id).storage_state == 'online')
        check('清退后容量不超额', storage.storage_overview(db)['used_bytes'] == 1000)
        check('新报告为待发布', db.get(Report, new.id).storage_state == 'pending')

        # new 上线后 RC 有一份在线；再制造每个草案都仅剩一份的局面。
        complete(new.id, 400)
        refresh(db)
        make_report(db, 'x', rc, 200)  # 清退 a1，RA 仅剩 a2 一份
        refresh(db)
        # 现状在线：RA=a2(200) RB=b0(200) RC=new(400)=800，pending x(200)。
        # 再申请 300：唯一可能被清退的是 RC(new)，但 RC 在线仅剩一份 → 不可清退 → 507。
        e = expect_http(507, lambda: make_report(db, 'stuck', ra, 300))
        check('无腾挪空间时 507', e is not None)
        check('原因含每草案保留一份', e is not None and '至少保留一份' in str(e))


def t3_download_protection():
    print('T3 下载期间腾空间：正在下载的不动')
    ra, rb, _ = setup_db()
    with Session() as db:
        a = [make_report(db, f'd{i}', ra, 200) for i in range(3)]
        for r in a:
            complete(r.id, 200)
        refresh(db)
        storage.begin_download(None, a[0].id)
        check('下载标记生效', a[0].id in storage.active_download_ids(None))
        # 申请 600：空闲仅 400，缺 200；a0 在下载不可动，a1 为次早且 RA 多份 → 清退 a1。
        make_report(db, 'need', rb, 600)
        refresh(db)
        check('正在下载的报告不被清退', db.get(Report, a[0].id).storage_state == 'online')
        check('改为清退次早可清退报告', db.get(Report, a[1].id).storage_state == 'evicted')
        storage.end_download(None, a[0].id)
        check('下载结束标记移除', a[0].id not in storage.active_download_ids(None))

    # HTTP 层行为：已清退 410 / 待发布 409 / 在线 200
    setup_db()
    app = FastAPI()
    app.include_router(reports_route.router)
    app.dependency_overrides[writer] = lambda: _User()
    app.dependency_overrides[current] = lambda: _User()
    client = TestClient(app)
    with Session() as db:
        ra = db.query(Relation).filter_by(code='RA').first().id
        ev = make_report(db, 'ev', ra, 200)
        complete(ev.id, 200)
        ev_id = ev.id
        pend = make_report(db, 'pend', ra, 200).id
        obj = db.get(Report, ev_id)
        obj.storage_state = 'evicted'
        obj.body_size = 0
        db.commit()
        ok = make_report(db, 'ok', ra, 200)
        complete(ok.id, 200)
        ok_id = ok.id
    check('已清退导出 410', client.get(f'/reports/{ev_id}/export').status_code == 410)
    check('待发布导出 409', client.get(f'/reports/{pend}/export').status_code == 409)
    r = client.get(f'/reports/{ok_id}/export')
    check('在线报告导出 200 带附件头',
          r.status_code == 200 and 'attachment' in r.headers.get('content-disposition', ''))


def t4_failure_retry_no_double_charge():
    print('T4 失败旧报告可下载 / 中断恢复 / 重试不重复扣占用')
    ra, _, _ = setup_db()
    with Session() as db:
        old = make_report(db, 'old', ra, 200)
        complete(old.id, 200)
        refresh(db)
        new = make_report(db, 'new', ra, 200)
        before = storage.storage_overview(db)
        storage.fail_publish(db, db.get(Report, new.id), '算法异常演示')
        db.commit()
        after = storage.storage_overview(db)
        check('失败后槽位与容量预留保留',
              after['used_slots'] == before['used_slots']
              and after['used_bytes'] == before['used_bytes'])
        check('失败报告仍待发布', db.get(Report, new.id).storage_state == 'pending')
        check('旧报告仍可下载', db.get(Report, old.id).storage_state == 'online')

        r = db.get(Report, new.id)
        token_before, size_before = r.slot_token, r.body_size
        fake = _FakeQueue()
        storage.retry_publish(db, r, fake, downloading_ids=set())
        db.expire_all()
        r = db.get(Report, new.id)
        check('重试复用同一额度标识', r.slot_token == token_before and r.body_size == size_before)
        check('重试只重新入队一条', fake.items == [str(new.id)])
        check('重试不增加占用', storage.storage_overview(db)['used_bytes'] == before['used_bytes'])
        complete(new.id, 200)
        db.expire_all()
        r = db.get(Report, new.id)
        check('重试后上线并释放槽位', r.storage_state == 'online' and r.slot_token == 0)


def t5_concurrent_slot_race():
    print('T5 多线程抢同一发布额度')
    setup_db()
    with Session() as db:
        rel = Relation(code='RX', title='t', attributes=['a', 'b'], note='')
        db.add(rel)
        db.commit()
        rel_id = rel.id
    storage.estimate_body_size = lambda r: 100  # type: ignore
    results, lock = [], threading.Lock()

    def attempt(i):
        try:
            # 与真实服务一致：整个"读配额→预留→落库"在全局存储锁临界区内。
            with storage.storage_lock(None):
                with Session() as db:
                    rel = db.get(Relation, rel_id)
                    snap = {'relation': {'id': rel.id, 'attributes': rel.attributes}, 'dependencies': []}
                    obj = Report(name=f'c{i}', relation_id=rel_id, input_snapshot=snap,
                                 status='queued', storage_state='pending')
                    storage.reserve_publish(db, obj, redis_client=None,
                                            downloading_ids=storage.active_download_ids(None))
                    db.add(obj)
                    db.commit()
                    with lock:
                        results.append(('ok', obj.id))
        except Exception as e:
            with lock:
                results.append(('reject', getattr(e, 'status_code', None)))

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = [x for x in results if x[0] == 'ok']
    reject = [x for x in results if x[0] == 'reject']
    check('成功数恰为并发额度 3', len(ok) == 3, f'ok={len(ok)}')
    check('其余 9 个被 429 拒绝', len(reject) == 9 and all(c == 429 for _, c in reject))
    with Session() as db:
        ov = storage.storage_overview(db)
        check('槽位占用不超额', ov['used_slots'] == 3 and ov['used_slots'] <= ov['publish_slots'])
        check('容量占用不超额', ov['used_bytes'] == 300 <= ov['limit_bytes'])
        check('待发布状态计数一致', ov['reports_by_state']['pending'] == 3)


def t6_actual_larger_than_reserved():
    print('T6 实际体量大於预留：补差额并触发清退')
    setup_db()
    with Session() as db:
        ry = Relation(code='RY', title='t', attributes=['a', 'b'], note='')
        rz = Relation(code='RZ', title='t2', attributes=['a'], note='')
        db.add_all([ry, rz])
        db.commit()
        ry_id, rz_id = ry.id, rz.id
        r1 = make_report(db, 'r1', ry_id, 300)
        r2 = make_report(db, 'r2', ry_id, 300)
        complete(r1.id, 300)
        complete(r2.id, 300)
        new = make_report(db, 'new', rz_id, 200)
        refresh(db)
        check('预留后占用 800', storage.storage_overview(db)['used_bytes'] == 800)
        # 实际 500（预留仅 200），补 300 差额：空闲 200 不够，清退最早 r1(300)。
        complete(new.id, 500)
        refresh(db)
        check('差额触发清退最早报告', db.get(Report, r1.id).storage_state == 'evicted')
        check('新报告按实际体量上线', db.get(Report, new.id).body_size == 500)
        # r2(300) + new(500) = 800
        check('多退少补后容量正确', storage.storage_overview(db)['used_bytes'] == 800)
        check('容量始终不超额', storage.storage_overview(db)['used_bytes'] <= 1000)


class _FakeQueue:
    def __init__(self):
        self.items = []

    def rpush(self, key, value):
        self.items.append(value)


def _test_app():
    app = FastAPI()
    app.include_router(reports_route.router)
    app.dependency_overrides[writer] = lambda: _User()
    app.dependency_overrides[current] = lambda: _User()
    return TestClient(app)


def t7_http_create_retry_overview():
    print('T7 HTTP 创建/容量视图/队列失败回滚/重试')
    ra, _, _ = setup_db()
    app = _test_app()
    # 真实 HTTP 创建（队列不可用）：预期 503，且不留悬挂占用。
    r = app.post('/reports', json={'name': 'http-create', 'relation_id': ra})
    check('队列不可用时创建返回 503', r.status_code == 503, r.text)
    with Session() as db:
        ov = storage.storage_overview(db)
        check('回滚后无悬挂槽位/容量占用', ov['used_slots'] == 0 and ov['used_bytes'] == 0)
        check('失败记录被标记为已清退不占容量',
              db.query(Report).count() == 1
              and db.query(Report).first().storage_state == 'evicted')

    # 容量视图结构
    r = app.get('/reports/storage/overview')
    body = r.json()
    check('容量视图字段完整', r.status_code == 200
          and all(k in body for k in ('limit_bytes', 'used_bytes', 'free_bytes',
                                      'publish_slots', 'used_slots', 'reports_by_state')))

    # 让队列可用（假队列），验证正常创建 + 重试端点复用额度。
    fake = _FakeQueue()
    report_service.queue = fake
    worker.queue = fake
    try:
        r = app.post('/reports', json={'name': 'ok-create', 'relation_id': ra})
        check('队列可用时创建 201', r.status_code == 201, r.text)
        created = r.json()
        check('新报告为待发布', created['storage_state'] == 'pending' and created['slot_token'] > 0)
        check('创建即入队一条', fake.items == [str(created['id'])])

        # 将其置为失败（模拟 worker 失败），再走 HTTP 重试
        with Session() as db:
            storage.fail_publish(db, db.get(Report, created['id']), '模拟失败')
            db.commit()
        token_before = created['slot_token']
        r = app.post(f"/reports/{created['id']}/retry")
        check('重试端点 200', r.status_code == 200, r.text)
        check('重试复用同一额度', r.json()['slot_token'] == token_before)
        check('重试累计入队两条（首条+重试）', fake.items == [str(created['id']), str(created['id'])])
        with Session() as db:
            ov = storage.storage_overview(db)
            check('重试不重复扣占用', ov['used_slots'] == 1)
    finally:
        # 还原为真实（不可达）队列，避免污染其它用例。
        from redis import Redis
        report_service.queue = Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
        worker.queue = report_service.queue


def t8_worker_pipeline():
    print('T8 worker 全链路：成功上线 / 失败保留额度 / 重复消息幂等')
    ra, _, _ = setup_db()
    # 固定较小体量，避免真实结果 JSON 撑爆测试用 1000 字节限额。
    storage.estimate_body_size = lambda r: 200  # type: ignore
    storage.actual_body_size = lambda res: 200  # type: ignore

    # 成功路径：预留 → worker._process → 在线
    with Session() as db:
        rid = make_report(db, 'w1', ra, 200).id
    worker._process(rid)
    with Session() as db:
        r = db.get(Report, rid)
        check('worker 成功后报告在线', r.storage_state == 'online' and r.slot_token == 0)
        check('worker 写入实际结果', r.result is not None and r.artifact_path)
        check('正文已落盘', os.path.exists(r.artifact_path))
    # 重复消息幂等：再次处理同 id 不应重复上线/报错
    worker._process(rid)
    with Session() as db:
        check('重复消息保持幂等', db.get(Report, rid).storage_state == 'online')

    # 失败路径：让 analyze 抛错，handle_failure 保留额度与预留
    with Session() as db:
        rid2 = make_report(db, 'w2', ra, 200).id
    orig = worker.analyze

    def boom(attrs, deps):
        raise ValueError('模拟算法失败')

    worker.analyze = boom
    try:
        worker.handle_failure(rid2, ValueError('模拟算法失败'))
    finally:
        worker.analyze = orig
    with Session() as db:
        r = db.get(Report, rid2)
        check('失败后仍待发布且保留额度', r.storage_state == 'pending' and r.slot_token > 0)
        check('失败原因被记录', '模拟算法失败' in (r.error or ''))
        ov = storage.storage_overview(db)
        check('失败仍占用预留容量与槽位', ov['used_bytes'] == 400 and ov['used_slots'] == 1)
    # 恢复后再次处理成功（中断恢复原发布）；用新会话读取以避免旧会话缓存。
    worker._process(rid2)
    with Session() as db:
        check('中断恢复后成功上线', db.get(Report, rid2).storage_state == 'online')


def t9_keep_one_within_single_eviction():
    print('T9 单次腾退不得把某草案清退到零份（边界回归）')
    ra, rb, _ = setup_db()
    with Session() as db:
        # RA 恰好两份在线（各 200），RB 一份在线（200）：共 600，空闲 400。
        ra_reports = [make_report(db, f'ra{i}', ra, 200) for i in range(2)]
        for x in ra_reports:
            complete(x.id, 200)
        rb0 = make_report(db, 'rb0', rb, 200)
        complete(rb0.id, 200)
        refresh(db)
        # 申请 600：缺 200。RA 只能清退最早一份（清退后仅剩一份必须保留），
        # RB 仅一份不可动；清退一份释放 200 恰好满足，RA 仍留一份在线。
        new = make_report(db, 'big', rb, 600)
        refresh(db)
        ra_online = [x for x in db.query(Report).filter_by(relation_id=ra).all()
                     if x.storage_state == 'online']
        check('RA 被清退后仍至少一份在线', len(ra_online) == 1)
        check('最早的 RA 报告被清退', db.get(Report, ra_reports[0].id).storage_state == 'evicted')
        check('较新的 RA 报告保留', db.get(Report, ra_reports[1].id).storage_state == 'online')
        check('RB 唯一报告保留', db.get(Report, rb0.id).storage_state == 'online')
        check('新报告待发布且容量不超额',
              db.get(Report, new.id).storage_state == 'pending'
              and storage.storage_overview(db)['used_bytes'] == 1000)


def main():
    print('临时目录：', _TMP)
    t1_capacity_and_slots()
    t2_eviction_order_and_keep_one()
    t3_download_protection()
    t4_failure_retry_no_double_charge()
    t5_concurrent_slot_race()
    t6_actual_larger_than_reserved()
    t7_http_create_retry_overview()
    t8_worker_pipeline()
    t9_keep_one_within_single_eviction()
    print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
    sys.exit(1 if FAIL else 0)


if __name__ == '__main__':
    main()
