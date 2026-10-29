"""Command parsing and mention stripping for dingtalk-selfask."""
import re

CMD_NEW, CMD_PUSH, CMD_STAT, CMD_SLASH = "new", "push", "status", "slash"
CMD_OVERVIEW = "overview"
CMD_OVERVIEW_KEYS = "overview_keys"


def strip_mention(text, self_name):
    """Remove leading DingTalk mention forms without stripping trailing payload."""
    value = text.lstrip()
    value = re.sub(r"^@%s\s*" % re.escape(self_name), "", value)
    return re.sub(r"^<@[^>]+>\s*", "", value)


def parse_cmd(body):
    """Return (kind, thread_number, payload) or (None, None, reason)."""
    # 裸 `aa`（整句就这两个字母）：本会话线程一览。`aa ` 带尾随空格仍算忽略。
    if body == "aa":
        return CMD_OVERVIEW, None, ""
    # `aa0`：和 `aa` 一样的一览，但每条多打一行会话 key，方便复制去 `/fork`。
    # 只认整句（允许尾随空白）；`aa0 x` 仍然按「0 号线程」忽略。
    if re.match(r"^aa0\s*$", body):
        return CMD_OVERVIEW_KEYS, None, ""
    if not body.startswith("aa"):
        return None, None, "not-aa"
    rest = body[2:]
    if rest.startswith(" "):
        payload = rest[1:].lstrip()
        if not payload:
            return None, None, "empty-after-aa"
        # `aa /xxx` 不再处理（2026-10-24 起斜杠只保留 `aaN /xxx` 一个入口）
        if payload.startswith("/"):
            return None, None, "slash-ignored"
        return CMD_NEW, None, payload
    match = re.match(r"^(\d{1,3})(\s+|$)", rest)
    if not match:
        return None, None, "not-aa"
    number = int(match.group(1))
    if number <= 0:
        return None, None, "aa-zero"
    payload = rest[match.end():].lstrip()
    if not payload:
        return CMD_STAT, number, ""
    return CMD_PUSH, number, payload
