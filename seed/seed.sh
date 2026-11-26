#!/bin/sh
# 向旧库预置样例会话（使用有写权限的 default 账号；应用账号只有读权限）。
# 幂等：重复执行会重置样例数据。
set -eu
HOST="${OLD_HOST:-127.0.0.1}"
PORT="${OLD_PORT:-6379}"
CLI="redis-cli -h $HOST -p $PORT"
SHORT_TTL="${SHORT_TTL:-45}"

echo "[seed] waiting for old redis at $HOST:$PORT ..."
until $CLI PING 2>/dev/null | grep -q PONG; do sleep 1; done

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "[seed] writing sample sessions ..."

# 24 个常规会话：2 小时有效期
i=1
while [ "$i" -le 24 ]; do
  n=$(printf "%02d" "$i")
  $CLI SETEX "session:s$n" 7200 \
    "{\"sid\":\"s$n\",\"user\":\"user$n\",\"created_at\":\"$now\",\"note\":\"sample session $n\"}" \
    > /dev/null
  i=$((i+1))
done

# 1 个短会话：用于演示“暂停期间过期的会话不能恢复”
$CLI SETEX "session:short-lived" "$SHORT_TTL" \
  "{\"sid\":\"short-lived\",\"user\":\"temp\",\"created_at\":\"$now\",\"note\":\"expires soon\"}" \
  > /dev/null

echo "[seed] done, 25 sessions. keys in old db:"
$CLI DBSIZE
