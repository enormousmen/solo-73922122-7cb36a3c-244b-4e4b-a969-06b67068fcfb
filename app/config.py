"""运行配置（全部可用环境变量覆盖）。"""
import os

# 旧库：迁移期间只读。docker-compose 中通过 ACL 账号强制只读。
REDIS_OLD_URL = os.environ.get("REDIS_OLD_URL", "redis://127.0.0.1:6380/0")
# 新库：迁移目标，也是迁移状态（游标/计数/开关）的存放处。
REDIS_NEW_URL = os.environ.get("REDIS_NEW_URL", "redis://127.0.0.1:6381/0")

# 每批 SCAN 的 COUNT 提示（每批实际条数以 Redis 返回为准）
MIGRATION_BATCH_COUNT = int(os.environ.get("MIGRATION_BATCH_COUNT", "5"))
# 自动搬运时每批之间的间隔，便于在网页上观察/暂停
MIGRATION_BATCH_DELAY_MS = int(os.environ.get("MIGRATION_BATCH_DELAY_MS", "300"))

SESSION_PREFIX = "session:"
STATE_KEY = "migration:state"
LOG_KEY = "migration:log"
