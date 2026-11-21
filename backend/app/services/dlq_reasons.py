"""死信失败原因筛选共享规则。

同一条原始消息可能经历两次失败：首次消费失败（原失败）与后续重放再次失败
（重放失败）。历史上两类原因被拼接在 ``失败原因`` 一个字段里，本模块统一把
它们还原为结构化的「失败原因明细」，并向检索清单、积压台账、值班复核台账与
集成监控提供同一份筛选规则，避免各模块各自按关键词归组导致口径不一致。

设计要点：
- 一条死信记录对应一条原始消息（入队时按消息指纹去重），后续失败以事件方式
  追加到 ``_failure_events``，因此同一消息在任何一轮分页里只可能出现一次。
- 筛选条件同时作用于原失败与重放失败事件；命中任一事件即命中该消息，消息
  本身不重复计数。
- 原因口径区分「处置」（消息仍待处置，原因可用于继续处置）与「回查」（消息
  已重放/已丢弃，原因只供事后回查）。
"""
from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Any

# 失败阶段
PHASE_ORIGINAL = "原失败"
PHASE_REPLAY = "重放失败"
PHASES = (PHASE_ORIGINAL, PHASE_REPLAY)

# 原因口径：可用于继续处置 / 只供回查
USABLE_ACTION = "处置"
USABLE_REFERENCE = "回查"

# 仍在积压、可继续处置的死信状态
PENDING_STATUSES = ("草稿", "待处置")
TERMINAL_STATUSES = ("已重放", "已丢弃")

# 历史拼接字段里的重放标记，如「数据格式异常 [重放重试]」
REPLAY_MARKER_RE = re.compile(r"\[\s*重放(?:重试|失败)?\s*\]|重放重试|重放失败", re.IGNORECASE)
# 合在一个字段里时常见的分段分隔符
SEGMENT_SPLIT_RE = re.compile(r"[;；|]")

# 原因关键词 → 原因分类（沿用既有归类口径，集中维护）
CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "超时类": ("超时", "timeout"),
    "格式类": ("格式", "format", "序列化"),
    "数据类": ("数据", "data", "空"),
    "连接类": ("连接", "网络", "connect"),
    "认证类": ("认证", "auth", "权限"),
}
CATEGORY_OTHER = "其他"

DEFAULT_REPLAY_REASON = "重放执行失败"
UNKNOWN_REASON = "未知原因"


def classify_reason(reason: str) -> str:
    """按关键词把单条原因归入原因分类。"""
    text = str(reason or "").casefold()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword.casefold() in text for keyword in keywords):
            return category
    return CATEGORY_OTHER


def _make_event(phase: str, reason: str, event_time: str = "", batch: str = "") -> dict[str, Any]:
    reason = str(reason or "").strip() or (DEFAULT_REPLAY_REASON if phase == PHASE_REPLAY else UNKNOWN_REASON)
    return {
        "phase": phase,
        "reason": reason,
        "category": classify_reason(reason),
        "time": event_time or "",
        "batch": batch or "",
    }


def parse_combined_reasons(raw_reason: str) -> list[dict[str, Any]]:
    """解析历史上合在 ``失败原因`` 一个字段里的多条原因。

    兼容两种历史写法：
    - 单段带重放标记：``原始格式有误 [重放重试]`` → 原失败（原始格式有误）
      + 重放失败（重放执行失败，标记本身不含具体原因）。
    - 多段拼接：``原始格式有误;重放超时`` → 首段为原失败，其余为重放失败；
      段内若带重放标记则先剥离标记再判定。
    """
    text = str(raw_reason or "").strip()
    if not text:
        return []

    segments = [seg.strip() for seg in SEGMENT_SPLIT_RE.split(text) if seg.strip()]
    if not segments:
        return []

    # 单段且带重放标记：同一字段同时承载原失败与一次重放失败
    if len(segments) == 1 and REPLAY_MARKER_RE.search(segments[0]):
        cleaned = REPLAY_MARKER_RE.sub("", segments[0]).strip(" -:：")
        return [
            _make_event(PHASE_ORIGINAL, cleaned or UNKNOWN_REASON),
            _make_event(PHASE_REPLAY, DEFAULT_REPLAY_REASON),
        ]

    events: list[dict[str, Any]] = []
    for index, segment in enumerate(segments):
        has_marker = bool(REPLAY_MARKER_RE.search(segment))
        cleaned = REPLAY_MARKER_RE.sub("", segment).strip(" -:：")
        phase = PHASE_ORIGINAL if index == 0 and not has_marker else PHASE_REPLAY
        events.append(_make_event(phase, cleaned))
    return events


def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """把持久化死信记录展开为带原因明细的统一视图（不写回存储）。"""
    events = row.get("_failure_events")
    if not isinstance(events, list) or not events:
        events = parse_combined_reasons(str(row.get("失败原因", "")))
    else:
        # 兼容事件里缺失分类/阶段的旧数据
        normalized_events: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            phase = event.get("phase") if event.get("phase") in PHASES else PHASE_ORIGINAL
            normalized_events.append(_make_event(
                phase,
                str(event.get("reason", "")),
                str(event.get("time", "")),
                str(event.get("batch", "")),
            ))
        events = normalized_events

    replay_events = [event for event in events if event["phase"] == PHASE_REPLAY]
    pending = row.get("status") in PENDING_STATUSES

    view = dict(row)
    view["失败原因明细"] = events
    view["原失败原因"] = next(
        (event["reason"] for event in events if event["phase"] == PHASE_ORIGINAL), None)
    view["重放失败原因"] = replay_events[-1]["reason"] if replay_events else None
    view["原因分类"] = sorted({event["category"] for event in events})
    view["可处置原因"] = pending
    view["原因口径"] = USABLE_ACTION if pending else USABLE_REFERENCE
    return view


def message_identity(row: dict[str, Any]) -> str:
    """原始消息身份：优先消息指纹，缺失时退回记录编号/ID。"""
    return str(row.get("_fingerprint") or row.get("记录编号") or row.get("id"))


@dataclass
class ReasonFilter:
    """死信原因筛选条件；检索清单与积压台账共用这一份规则。"""

    keyword: str | None = None          # 原因文本关键词，命中原失败或重放失败均可
    category: str | None = None         # 原因分类，如 超时类/格式类
    phase: str | None = None            # 失败阶段：原失败/重放失败
    usable: str | None = None           # 原因口径：处置/回查
    queue: str | None = None            # 原始队列（源队列数据）
    status: str | None = None           # 处置状态

    def as_dict(self) -> dict[str, str]:
        """稳定序列化结果，用于分页边界绑定筛选条件。"""
        return {
            "keyword": (self.keyword or "").strip(),
            "category": (self.category or "").strip(),
            "phase": (self.phase or "").strip(),
            "usable": (self.usable or "").strip(),
            "queue": (self.queue or "").strip(),
            "status": (self.status or "").strip(),
        }

    def _event_matches(self, event: dict[str, Any]) -> bool:
        if self.keyword and self.keyword.casefold() not in str(event.get("reason", "")).casefold():
            return False
        if self.category and event.get("category") != self.category:
            return False
        if self.phase and event.get("phase") != self.phase:
            return False
        return True

    def matches(self, view: dict[str, Any]) -> bool:
        if self.status and view.get("status") != self.status:
            return False
        if self.queue and self.queue.casefold() not in str(view.get("原始队列", "")).casefold():
            return False
        if self.usable:
            # 「处置」口径只看待处置消息；「回查」口径只看已处置（重放/丢弃）消息
            if self.usable == USABLE_ACTION and not view.get("可处置原因"):
                return False
            if self.usable == USABLE_REFERENCE and view.get("可处置原因"):
                return False
        # 原因类条件：任一失败事件命中即命中整条消息
        reason_conditions = [self.keyword, self.category, self.phase]
        if any(condition for condition in reason_conditions):
            return any(self._event_matches(event) for event in view.get("失败原因明细", []))
        return True

    def has_reason_condition(self) -> bool:
        return bool(self.keyword or self.category or self.phase)

    def matching_events(self, view: dict[str, Any]) -> list[dict[str, Any]]:
        """返回该消息上命中原因筛选条件的事件；无条件时返回全部事件。"""
        events = view.get("失败原因明细", [])
        if not self.has_reason_condition():
            return list(events)
        return [event for event in events if self._event_matches(event)]


def build_reason_filter(filters: dict[str, str] | None, status: str | None = None) -> ReasonFilter:
    """从资源路由的查询参数构造原因筛选条件。"""
    filters = filters or {}
    return ReasonFilter(
        keyword=str(filters.get("失败原因", "")).strip() or None,
        category=str(filters.get("原因分类", "")).strip() or None,
        phase=str(filters.get("失败阶段", "")).strip() or None,
        usable=str(filters.get("原因口径", "")).strip() or None,
        queue=str(filters.get("原始队列", "")).strip() or None,
        status=status,
    )


class DlqReasonView:
    """死信原因只读视图：原因明细、筛选、原因选项与积压汇总的统一取数入口。"""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        # 同一原始消息只保留一条视图（结构性去重，并发追加原因也不重复）
        deduped: dict[str, dict[str, Any]] = {}
        for row in rows:
            view = normalize_record(row)
            deduped.setdefault(message_identity(view), view)
        self._views = list(deduped.values())
        self._views.sort(key=lambda item: (str(item.get("入队时间", "")), int(item.get("id", 0))))

    @property
    def views(self) -> list[dict[str, Any]]:
        return self._views

    def filtered(self, reason_filter: ReasonFilter | None = None) -> list[dict[str, Any]]:
        reason_filter = reason_filter or ReasonFilter()
        return [view for view in self._views if reason_filter.matches(view)]

    def reason_options(self, reason_filter: ReasonFilter | None = None) -> list[dict[str, Any]]:
        """原因筛选下拉/分组数据：按「阶段+原因+分类」归组并标注口径计数。

        - 消息数按原始消息身份去重：同一消息即使同时带原失败与重放失败，
          在每个原因分组里也只计一次；
        - 带原因类筛选条件时，只统计命中条件的原因事件，分组口径与筛选一致。
        """
        reason_filter = reason_filter or ReasonFilter()
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for view in self.filtered(reason_filter):
            pending = bool(view.get("可处置原因"))
            seen_groups: set[tuple[str, str, str]] = set()
            for event in reason_filter.matching_events(view):
                key = (event["phase"], event["reason"], event["category"])
                if key in seen_groups:
                    continue
                seen_groups.add(key)
                bucket = grouped.setdefault(key, {
                    "失败阶段": key[0],
                    "失败原因": key[1],
                    "原因分类": key[2],
                    "消息数": 0,
                    "可处置消息数": 0,
                    "回查消息数": 0,
                    "原因口径": USABLE_ACTION if pending else USABLE_REFERENCE,
                    "dlq_ids": [],
                })
                bucket["消息数"] += 1
                bucket["可处置消息数"] += 1 if pending else 0
                bucket["回查消息数"] += 0 if pending else 1
                if view.get("id") not in bucket["dlq_ids"]:
                    bucket["dlq_ids"].append(view.get("id"))
                # 同一原因下既有积压又有已处置记录时，口径标注为混合
                if bucket["原因口径"] != (USABLE_ACTION if pending else USABLE_REFERENCE):
                    bucket["原因口径"] = "处置+回查"
        options = list(grouped.values())
        for option in options:
            option["dlq_ids"] = sorted(set(option["dlq_ids"]))
        options.sort(key=lambda item: (-item["消息数"], item["失败阶段"], item["失败原因"]))
        return options

    def backlog_summary(self, reason_filter: ReasonFilter | None = None) -> dict[str, Any]:
        """积压台账：与检索清单同一份筛选规则，总量按原始消息去重计算。"""
        reason_filter = reason_filter or ReasonFilter()
        matched = self.filtered(reason_filter)
        pending_views = [view for view in matched if view.get("可处置原因")]
        messages = {message_identity(view) for view in pending_views}
        # 积压口径只保留仍待处置消息上的原因分组（处置口径，消息已按消息身份去重）
        backlog_groups = [
            {**group, "消息数": group["可处置消息数"], "回查消息数": 0,
             "原因口径": USABLE_ACTION}
            for group in self.reason_options(reason_filter)
            if group["可处置消息数"] > 0
        ]
        by_queue: dict[str, int] = {}
        for view in pending_views:
            queue = str(view.get("原始队列", "未知"))
            by_queue[queue] = by_queue.get(queue, 0) + 1
        return {
            "筛选口径": (reason_filter or ReasonFilter()).as_dict(),
            "积压消息总数": len(messages),
            "积压记录数": len(pending_views),
            "按原始队列": dict(sorted(by_queue.items(), key=lambda item: (-item[1], item[0]))),
            "原因分组": backlog_groups,
        }


# ─────────────────────────────────────────────────────────────
# 分页边界：同时绑定原死信身份（末条记录ID）与实际筛选条件
# ─────────────────────────────────────────────────────────────

def encode_cursor(last_id: int, reason_filter: ReasonFilter) -> str:
    payload = {"id": int(last_id), "filter": reason_filter.as_dict()}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(cursor: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("分页游标格式无效") from exc
    if not isinstance(payload, dict) or "id" not in payload or "filter" not in payload:
        raise ValueError("分页游标内容不完整")
    return payload
