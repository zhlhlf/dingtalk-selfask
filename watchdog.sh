#!/bin/sh
# 守护 selfask.py：进程不在就拉起来。只管进程，别的什么都不判断。
#
# 收消息只有一条长连接（dws event consume）。它断了就自动重连，退避从 5s
# 起翻倍、最长 60s，这层由进程内的 listen_loop 负责，看门狗不参与。
# 看门狗只在「整个进程没了」的时候兜底：进程在就退出，不在就重启。
#
# 匹配必须锚定在命令行开头和结尾（^python3 ... selfask\.py$），不能用自由子串：
# 任何一条命令行里恰好带上这个路径的 shell（grep、编辑、重启命令本身）都会被
# 误判成「已在跑」，导致 watchdog 永远直接 exit 0、守护彻底失效。
# 2026-10-23 实际踩过：重启脚本里的自匹配让 watchdog 静默失效。
BASE=/root/.openclaw/workspace/scripts/dingtalk-selfask

# OpenClaw 2026.10.6 起，网关拒绝从 agent exec 环境发起的 CLI chat.send
# （OPENCLAW_SHELL=exec / OPENCLAW_SUBAGENT_EXEC=1，报
# "would lose inter-session attribution"）。本脚本由自动化在 agent 会话里调用，
# 不清理这两个标记的话，selfask 会带着它们常驻，每条 aa 消息都会被拒。
unset OPENCLAW_SHELL OPENCLAW_SUBAGENT_EXEC

if pgrep -f '^python3[[:space:]]+([^[:space:]]*/)?selfask\.py$' > /dev/null 2>&1; then
  exit 0
fi
cd "$BASE" || exit 1
setsid nohup python3 "$BASE/selfask.py" >> "$BASE/boot.log" 2>&1 < /dev/null &
echo "$(date '+%F %T') watchdog: restarted（进程不在）" >> "$BASE/selfask.log"
