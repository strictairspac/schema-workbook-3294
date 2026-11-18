import json

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from src.config.database import db_session
from src.utils.security import current, writer
from src.schemas.report import ReportInput
from src.services import reports as service
from src.services import storage

router = APIRouter()


@router.get('/reports')
def listing(db=Depends(db_session), user=Depends(current)):
    return service.listing(db)


@router.get('/reports/storage/overview')
def overview(db=Depends(db_session), user=Depends(current)):
    return service.overview(db)


@router.post('/reports', status_code=201)
def create(data: ReportInput, db=Depends(db_session), user=Depends(writer)):
    return service.create(db, data, user)


@router.post('/reports/{ident}/retry', status_code=200)
def retry(ident: int, db=Depends(db_session), user=Depends(writer)):
    return service.retry(db, ident, user)


@router.get('/reports/{ident}')
def detail(ident: int, db=Depends(db_session), user=Depends(current)):
    return service.detail(db, ident)


@router.get('/reports/{ident}/export')
def export(ident: int, db=Depends(db_session), user=Depends(current)):
    obj = service.detail(db, ident)
    # 已清退或尚未上线的正文不可下载；给出明确原因而非返回空文件。
    if obj['storage_state'] == 'evicted':
        raise HTTPException(410, '该报告正文已被清退以释放在线存储空间，任务记录保留但正文不可下载')
    if obj['storage_state'] != 'online' or not obj.get('result'):
        raise HTTPException(409, '报告正文尚未发布完成，请等待发布成功后再下载')

    client = service._client()
    # 下载期间加保护标记：任何清退都不得动正在下载的正文。
    storage.begin_download(client, ident)
    try:
        # 重新读取，确保下载窗口期内基于最新状态判定。
        fresh = service.detail(db, ident)
        if fresh['storage_state'] != 'online':
            raise HTTPException(410, '报告正文在下载前已被清退，请稍后重试')
        payload = json.dumps(fresh, ensure_ascii=False, indent=2)
        return Response(
            payload,
            media_type='application/json',
            headers={'Content-Disposition': f'attachment; filename="report-{ident}.json"'},
        )
    finally:
        storage.end_download(client, ident)
