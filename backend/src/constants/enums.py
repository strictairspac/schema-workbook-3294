ROLES = ('admin', 'operator', 'viewer')
# 任务执行状态（worker 计算流水线）
RUN_STATES = ('queued', 'running', 'completed', 'failed')
# 报告正文在线存储状态：
#   pending  待发布：已占用发布额度，正文尚未上线（queued/running/failed 未完成发布）
#   online   可下载：正文已发布并占用存储容量
#   evicted  已清退：正文因腾挪容量被清退，任务记录保留但不可下载
STORAGE_STATES = ('pending', 'online', 'evicted')
