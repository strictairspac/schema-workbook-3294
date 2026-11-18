import os
DATABASE_URL=os.getenv('DATABASE_URL','mysql+pymysql://schemaworkbook:schemaworkbook_demo_pw@localhost:17743/schemaworkbook')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-schema-workbook-change-before-public-use')
# 报告正文在线存储配额：总容量(字节)与并发发布额度。可由环境变量覆盖。
STORAGE_LIMIT_BYTES=int(os.getenv('STORAGE_LIMIT_BYTES',2*1024*1024))
PUBLISH_SLOTS=int(os.getenv('PUBLISH_SLOTS',3))
# 估算报告正文大小前的采样上限，以及存储锁/锁目录。
LOCK_DIR=os.getenv('STORAGE_LOCK_DIR','/tmp/schemaworkbook-locks')
# 报告正文落盘目录（容器内默认 /artifacts，可由环境变量覆盖）。
ARTIFACT_DIR=os.getenv('ARTIFACT_DIR','/artifacts')
