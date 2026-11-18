import json
import traceback
from pathlib import Path

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models.entities import Report
from src.services.reports import queue, _client
from src.services import storage
from src.services.normalization import analyze

ARTIFACT_PATH = Path(ARTIFACT_DIR)


def handle_failure(ident: int, error: Exception):
    """发布未成功：保留额度与容量预留，旧报告仍可下载，等待重试。"""
    with Session() as db:
        with storage.storage_lock(_client()):
            obj = db.get(Report, ident)
            if obj and obj.storage_state == 'pending' and obj.slot_token:
                storage.fail_publish(db, obj, str(error))
                db.commit()


def _process(ident: int):
    client = _client()
    with Session() as db:
        obj = db.get(Report, ident)
        if obj is None:
            return
        # 仅待发布且仍持有额度的报告才执行；已上线/已清退的重复消息直接忽略，
        # 保证 at-least-once 投递下不会重复扣占用、重复上线。
        if obj.storage_state != 'pending' or not obj.slot_token:
            return
        if obj.status not in ('queued', 'failed'):
            # running 等中间态（例如崩溃残留）允许恢复，继续走原发布。
            pass
        obj.status = 'running'
        obj.storage_note = '发布进行中：占用原并发额度与容量预留，旧报告仍可下载'
        db.commit()
        snapshot = obj.input_snapshot

    # 重计算在锁外执行，避免长任务持有全局存储锁。
    result = analyze(snapshot['relation']['attributes'], snapshot['dependencies'])

    ARTIFACT_PATH.mkdir(parents=True, exist_ok=True)
    artifact_path = ARTIFACT_PATH / f'report-{ident}.json'
    artifact_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    with Session() as db:
        with storage.storage_lock(client):
            obj = db.get(Report, ident)
            if obj is None or obj.storage_state != 'pending' or not obj.slot_token:
                # 并发保护：状态已被其他流程改变，则放弃本次上线，避免重复占用。
                return
            storage.complete_publish(
                db, obj, result, str(artifact_path),
                redis_client=client,
                downloading_ids=storage.active_download_ids(client),
            )
            db.commit()


def main():
    while True:
        Path('/tmp/worker-heartbeat').touch()
        item = queue.blpop('schemaworkbook:queue', timeout=2)
        if not item:
            continue
        ident = int(item[1])
        try:
            _process(ident)
        except Exception as e:
            traceback.print_exc()
            handle_failure(ident, e)


if __name__ == '__main__':
    main()
