"""死信队列管理领域服务：失败消息隔离、原因分类分析、批量重放与处置策略。

提供完整的死信处理流程：消息入队→原因归类→处置决策→批量重放→结果验证。
跨模块关联：mq_consumer（来源队列）、call_trace_log（重放追踪）、
data_quality_rule（质量校验）、integration_monitor（原因筛选数据跨模块消费）。

原因模型（见 :mod:`app.services.dlq_reasons`）：
- 一条死信记录对应一条原始消息（入队按消息指纹去重）；
- 原失败与重放失败以事件形式追加到 ``_failure_events``，不再拼接进单个字段；
- 检索、积压台账、值班复核与集成监控共用同一份原因筛选规则。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.dlq_reasons import (
    DEFAULT_REPLAY_REASON,
    PENDING_STATUSES,
    PHASE_ORIGINAL,
    PHASE_REPLAY,
    ReasonFilter,
    USABLE_ACTION,
    USABLE_REFERENCE,
    build_reason_filter,
    decode_cursor,
    encode_cursor,
    message_identity,
    DlqReasonView,
)

REVIEW_LEDGER_KEY = "dlq_review_ledger"


class DlqHandlerService(BaseModuleService):
    MODULE = "dlq_handler"
    ENTITY = "死信记录"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["记录编号", "原始队列", "失败原因", "原始消息体", "重试次数", "入队时间", "处置状态", "重放批次"]
    # 资源路由额外放行的死信原因筛选参数（不属于通用列表字段）
    REASON_FILTER_FIELDS = ["原因分类", "失败阶段", "原因口径"]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "失败原因"]
    STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
    ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._replay_history: list[dict[str, Any]] = []

    # ──────────────────────────────────────────────
    # 入队：原失败结构化记录，重复消息追加原因事件
    # ──────────────────────────────────────────────
    def enqueue_dead_letter(self, source_queue: str, original_message: str,
                            failure_reason: str, consumer_id: int = 0) -> dict[str, Any]:
        """将消费失败的消息加入死信队列。

        自动生成记录编号、消息指纹、入队时间戳和失败原因分类。
        相同指纹的消息重复入队时不再覆盖原因，而是追加一条失败事件，
        保证同一原始消息在任何分页结果里只出现一次。
        """
        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        msg_fingerprint = hashlib.sha256(original_message.encode()).hexdigest()[:16]

        # 检查是否有相同指纹的已有死信（避免重复入队；并发追加原因后重新查询仍只有一条）
        duplicate = [e for e in existing if e.get("_fingerprint") == msg_fingerprint and e.get("status") in PENDING_STATUSES]
        if duplicate:
            existing_dlq = duplicate[0]
            retry_count = int(existing_dlq.get("重试次数", "0") or "0") + 1
            events = list(existing_dlq.get("_failure_events", []))
            events.append({
                "phase": PHASE_ORIGINAL,
                "reason": failure_reason,
                "time": datetime.now(timezone.utc).isoformat(),
                "batch": "",
            })
            json_store.update(self.MODULE, int(existing_dlq["id"]), {
                "重试次数": str(retry_count),
                "_failure_events": events,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            return {
                "ok": True,
                "message": f"消息已在死信队列中（retry#{retry_count}），原因已追加到记录 #{existing_dlq['id']}",
                "dlq_id": existing_dlq["id"],
                "is_duplicate": True,
            }

        record_no = f"DLQ-{source_queue[:4].upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"
        now = datetime.now(timezone.utc).isoformat()

        dlq_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "原始队列": source_queue,
            "失败原因": failure_reason,
            "原始消息体": original_message[:200],
            "重试次数": "0",
            "入队时间": now,
            "处置状态": "草稿",
            "重放批次": "",
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": now,
            "_fingerprint": msg_fingerprint,
            "_consumer_id": consumer_id,
            # 结构化原因明细：原失败与重放失败分别记录，兼容只存拼接字段的历史数据
            "_failure_events": [{
                "phase": PHASE_ORIGINAL,
                "reason": failure_reason,
                "time": now,
                "batch": "",
            }],
        }

        json_store.insert(self.MODULE, dlq_entry)
        return {"ok": True, "message": f"死信已入队，编号 {record_no}", "dlq_id": dlq_entry["id"], "is_duplicate": False}

    def _reason_view(self) -> DlqReasonView:
        return DlqReasonView(json_store.get_all(self.MODULE))

    # ──────────────────────────────────────────────
    # 统一取数：检索清单（含原因筛选、游标分页、消息级去重）
    # ──────────────────────────────────────────────
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None]:
        """按原因筛选规则检索死信记录。

        - 筛选同时作用于原失败与重放失败事件，命中任一事件即命中该消息；
        - 同一原始消息（指纹/记录编号）在一轮分页结果只计一次；
        - 游标分页同时携带原死信身份（末条ID）与实际筛选条件，翻页时校验口径。
        """
        reason_filter = build_reason_filter(filters, status=status)
        if keyword:
            reason_filter.keyword = keyword

        view = self._reason_view()
        matched = view.filtered(reason_filter)
        total = len(matched)

        start = (page - 1) * size
        next_cursor: str | None = None

        if cursor:
            payload = decode_cursor(cursor)
            if payload.get("filter") != reason_filter.as_dict():
                raise ValueError("分页游标与当前筛选条件不一致，请回到第一页重新检索")
            last_id = int(payload["id"])
            page_rows = [row for row in matched if int(row.get("id", 0)) > last_id][:size]
        else:
            page_rows = matched[start:start + size]

        if page_rows and (matched.index(page_rows[-1]) + 1) < total:
            next_cursor = encode_cursor(int(page_rows[-1]["id"]), reason_filter)

        return page_rows, total, next_cursor

    def get_reason_options(self, filters: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """原因筛选数据：按阶段+原因+分类分组，标注处置/回查口径与消息数。"""
        reason_filter = build_reason_filter(filters)
        return self._reason_view().reason_options(reason_filter)

    def get_backlog_summary(self, filters: dict[str, str] | None = None) -> dict[str, Any]:
        """积压台账：与检索清单共用同一份筛选规则，总量按原始消息计算。"""
        reason_filter = build_reason_filter(filters)
        return self._reason_view().backlog_summary(reason_filter)

    def analyze_failure_reasons(self) -> dict[str, Any]:
        """分析所有死信记录的失败原因分布（按原因明细事件统计，兼容历史拼接字段）。"""
        view = self._reason_view()
        rows = view.views
        if not rows:
            return {"total": 0, "categories": {}, "top_reasons": [], "top_sources": [], "pending_count": 0}

        # 原因维度按失败事件计数；消息维度按原始消息计数
        reason_counter: dict[str, int] = {}
        category_counter: dict[str, int] = {}
        source_counter: dict[str, int] = {}
        for row in rows:
            source_counter[str(row.get("原始队列", "未知"))] = source_counter.get(str(row.get("原始队列", "未知")), 0) + 1
            for event in row.get("失败原因明细", []):
                reason_counter[event["reason"]] = reason_counter.get(event["reason"], 0) + 1
                category_counter[event["category"]] = category_counter.get(event["category"], 0) + 1

        return {
            "total": len(rows),
            "categories": {k: v for k, v in sorted(category_counter.items(), key=lambda item: -item[1]) if v > 0},
            "top_reasons": sorted(reason_counter.items(), key=lambda item: -item[1])[:5],
            "top_sources": sorted(source_counter.items(), key=lambda item: -item[1])[:5],
            "pending_count": sum(1 for row in rows if row.get("status") in PENDING_STATUSES),
            "reason_options": view.reason_options(),
        }

    def batch_replay(self, dlq_ids: list[int], replay_reason: str | None = None,
                     force_fail: bool = False) -> dict[str, Any]:
        """批量重放死信队列中的消息。

        流程：
        1. 验证每条死信记录是否存在且处于可重放状态
        2. 生成重放批次ID
        3. 逐条尝试重放到原始队列
        4. 重放失败时把重放失败原因作为独立事件追加（不覆盖原失败原因）

        返回重放统计和每条消息的明细结果。
        """
        if not dlq_ids:
            return {"ok": False, "message": "未指定待重放的死信记录", "replayed": 0, "failed": 0}

        batch_id = f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        results = []
        replayed = 0
        replay_failed = 0

        import random
        for dlq_id in dlq_ids:
            entry = json_store.get_by_id(self.MODULE, dlq_id)
            if entry is None:
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "死信记录不存在"})
                replay_failed += 1
                continue

            if entry.get("status") in ("已重放", "已丢弃"):
                results.append({"dlq_id": dlq_id, "ok": False, "reason": f"记录已{entry['status']}，不可重复重放"})
                replay_failed += 1
                continue

            # 模拟重放：70%成功率；force_fail 供测试与故障演练确定性走失败分支
            success = (random.random() < 0.7) and not force_fail

            if success:
                json_store.update(self.MODULE, dlq_id, {
                    "status": "已重放",
                    "处置状态": "已重放",
                    "pending": False,
                    "abnormal": False,
                    "重放批次": batch_id,
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "replayed_at": datetime.now(timezone.utc).isoformat(),
                })
                replayed += 1
                results.append({"dlq_id": dlq_id, "ok": True, "message": "重放成功"})
            else:
                # 重放失败：重试计数+1，保持待处置；重放原因独立追加，原失败原因保留
                now = datetime.now(timezone.utc).isoformat()
                reason_text = replay_reason or DEFAULT_REPLAY_REASON
                events = list(entry.get("_failure_events", []))
                events.append({
                    "phase": PHASE_REPLAY,
                    "reason": reason_text,
                    "time": now,
                    "batch": batch_id,
                })
                json_store.update(self.MODULE, dlq_id, {
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "status": "待处置",
                    "处置状态": "待处置",
                    "pending": True,
                    "abnormal": True,
                    "_failure_events": events,
                    "重放失败原因": reason_text,
                    "updated_at": now,
                })
                replay_failed += 1
                results.append({"dlq_id": dlq_id, "ok": False, "reason": reason_text})

        # 记录重放历史
        self._replay_history.append({
            "batch_id": batch_id,
            "total": len(dlq_ids),
            "replayed": replayed,
            "failed": replay_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # 跨模块：记录重放到调用追踪
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=0,
                action="死信批量重放",
                latency_ms=random.randint(100, 500),
                status="正常" if replay_failed == 0 else "异常",
                record_count=replayed,
            )
        except Exception:
            pass

        return {
            "ok": True,
            "message": f"批量重放完成: 成功 {replayed}/{len(dlq_ids)}, 失败 {replay_failed}",
            "batch_id": batch_id,
            "replayed": replayed,
            "failed": replay_failed,
            "total": len(dlq_ids),
            "results": results,
        }

    # ──────────────────────────────────────────────
    # 状态流转：死信数据落在 json_store，需覆盖基类基于内存 store 的实现
    # ──────────────────────────────────────────────
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = json_store.get_by_id(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        updates: dict[str, Any] = {
            "status": target,
            "处置状态": target,
            "pending": target != self.STATUS_ORDER[-1],
            "abnormal": action in self.NEGATIVE_ACTIONS,
        }
        if target in ("已重放", "已丢弃"):
            updates["pending"] = False
        updated = json_store.update(self.MODULE, entry_id, updates)
        return updated, f"{self.ENTITY}已{action}"

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = json_store.get_by_id(self.MODULE, entry_id)
        from app.services.dlq_reasons import normalize_record
        return normalize_record(entry) if entry else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """通用登记入口：补齐记录编号后走入队流程，保证原因明细结构一致。"""
        missing = [field for field in self.REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        result = self.enqueue_dead_letter(
            source_queue=str(values.get("原始队列", "unknown")),
            original_message=str(values.get("原始消息体") or values.get("记录编号", "")),
            failure_reason=str(values.get("失败原因", "")),
        )
        return json_store.get_by_id(self.MODULE, int(result["dlq_id"])), []

    # ──────────────────────────────────────────────
    # 值班复核台账：分组结果落账，签收后仍可回查当时口径与两类失败
    # ──────────────────────────────────────────────
    def create_review(self, team: str, filters: dict[str, str] | None = None,
                      remark: str = "") -> dict[str, Any]:
        """从某个班组的原因筛选结果发起积压复核，分组快照写入值班复核台账。"""
        reason_filter = build_reason_filter(filters)
        view = self._reason_view()
        matched = view.filtered(reason_filter)

        # 班组复核对象：处置口径只看待处置消息；回查口径看全部命中消息
        if reason_filter.usable == USABLE_REFERENCE:
            scope_views = matched
        else:
            scope_views = [item for item in matched if item.get("可处置原因")]

        usable = reason_filter.usable
        if not usable:
            usable = USABLE_ACTION if any(item.get("可处置原因") for item in scope_views) else USABLE_REFERENCE

        snapshot = [{
            "dlq_id": item.get("id"),
            "记录编号": item.get("记录编号"),
            "原始队列": item.get("原始队列"),
            "原失败原因": item.get("原失败原因"),
            "重放失败原因": item.get("重放失败原因"),
            "失败原因明细": item.get("失败原因明细"),
            "处置状态": item.get("status"),
            "消息身份": message_identity(item),
        } for item in scope_views]

        ledger = json_store.get_all(REVIEW_LEDGER_KEY)
        next_id = max((int(row.get("id", 0)) for row in ledger), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        record = {
            "id": next_id,
            "台账编号": f"REV-{next_id:05d}",
            "班组名称": team,
            "原因口径": usable,
            "筛选口径": reason_filter.as_dict(),
            "原因分组": view.reason_options(reason_filter),
            "消息快照": snapshot,
            "积压消息总数": len({message_identity(item) for item in scope_views}),
            "复核状态": "待签收",
            "签收人": "",
            "签收时间": "",
            "备注": remark or "",
            "status": "待签收",
            "pending": True,
            "abnormal": False,
            "created_at": now,
        }
        json_store.insert(REVIEW_LEDGER_KEY, record)
        return record

    def list_reviews(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = json_store.get_all(REVIEW_LEDGER_KEY)
        if status:
            rows = [row for row in rows if row.get("复核状态") == status or row.get("status") == status]
        return sorted(rows, key=lambda item: -int(item.get("id", 0)))

    def get_review(self, review_id: int) -> dict[str, Any] | None:
        return json_store.get_by_id(REVIEW_LEDGER_KEY, review_id)

    def sign_review(self, review_id: int, signer: str) -> tuple[dict[str, Any] | None, str]:
        """签收复核台账；签收后筛选口径、原失败与重放失败快照仍可重新定位。"""
        record = json_store.get_by_id(REVIEW_LEDGER_KEY, review_id)
        if record is None:
            return None, f"复核台账 {review_id} 不存在"
        if record.get("复核状态") == "已签收":
            return None, f"复核台账 {record.get('台账编号')} 已签收，不可重复签收"
        now = datetime.now(timezone.utc).isoformat()
        updated = json_store.update(REVIEW_LEDGER_KEY, review_id, {
            "复核状态": "已签收",
            "status": "已签收",
            "pending": False,
            "签收人": signer,
            "签收时间": now,
        })
        return updated, f"复核台账 {record.get('台账编号')} 已由 {signer} 签收"

    def get_dlq_stats(self) -> dict[str, Any]:
        """获取死信队列的全局统计信息。"""
        view = self._reason_view()
        rows = view.views
        by_status: dict[str, int] = {}
        by_queue: dict[str, int] = {}
        for row in rows:
            s = row.get("status", "未知")
            q = row.get("原始队列", "未知")
            by_status[s] = by_status.get(s, 0) + 1
            by_queue[q] = by_queue.get(q, 0) + 1

        return {
            "total_dlq": len(rows),
            "by_status": by_status,
            "by_queue": dict(sorted(by_queue.items(), key=lambda x: x[1], reverse=True)[:10]),
            "pending_dispatch": by_status.get("草稿", 0) + by_status.get("待处置", 0),
            "replayed_count": by_status.get("已重放", 0),
            "discarded_count": by_status.get("已丢弃", 0),
            "analysis": self.analyze_failure_reasons(),
            "backlog": view.backlog_summary(ReasonFilter()),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出死信队列管理的数据质量快照。"""
        view = self._reason_view()
        rows = view.views
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        analysis = self.analyze_failure_reasons()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信队列记录数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_replays": len(self._replay_history),
            "failure_categories": analysis.get("categories", {}),
            "reason_options": analysis.get("reason_options", []),
            "backlog_total": view.backlog_summary(ReasonFilter())["积压消息总数"],
            "operation": self.OPERATION_TERM,
        }
