import os,json
from pathlib import Path
CONFIG=json.loads(Path(os.getenv('PROJECT_JSON','/app/project.json')).read_text())
DATABASE_URL=os.environ['DATABASE_URL']
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
SECRET=os.environ['JWT_SECRET']
# 租约时长（秒）：超过该时长未续租的执行者，其任务可被他人接管
WORKER_LEASE_SECONDS=float(os.getenv('WORKER_LEASE_SECONDS','30'))
