"""死信原因筛选、积压台账与值班复核台账测试。

覆盖需求场景：
1. 同一条消息先后原失败（格式有误）与重放失败（超时），两类原因分别可筛；
2. 历史上合在一个字段里的原因（含 [重放重试] 标记/多段拼接）可兼容解析；
3. 并发/重复追加原因后重新查询，同一消息一轮分页只计一次；
4. 分页边界同时绑定原死信身份与实际筛选条件，游标与条件不一致被拒绝；
5. 积压台账与检索清单共用筛选规则，积压总数按原始消息计算；
6. 处置/回查两种原因口径可区分，状态流转后口径随之切换；
7. 两个班组分别从各自原因筛选结果发起复核，签收后仍可重新定位筛选口径、
   原失败与重放失败；
8. 集成监控跨模块复用同一份死信原因筛选数据，并可写入值班复核台账。
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore, json_store as global_store
import app.persistence as persistence
import app.services.dlq_handler as dlq_module
import app.services.integration_monitor as monitor_module
from app.services.dlq_reasons import (
    PHASE_ORIGINAL,
    PHASE_REPLAY,
    USABLE_ACTION,
    USABLE_REFERENCE,
    ReasonFilter,
    build_reason_filter,
    decode_cursor,
    encode_cursor,
    parse_combined_reasons,
)


class DlqReasonTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = JsonStore(base_dir=self._tmp.name)
        # 服务模块在 import 时已绑定 json_store 名字，需先换存储再 reload，
        # 保证每个用例拿到独立的空分区。
        persistence.json_store = self.store
        import importlib
        importlib.reload(dlq_module)
        importlib.reload(monitor_module)
        self.svc = dlq_module.DlqHandlerService()

    def tearDown(self) -> None:
        # 恢复全局存储与模块绑定，避免 reload 副作用污染后续测试套件
        import importlib
        persistence.json_store = global_store
        importlib.reload(dlq_module)
        importlib.reload(monitor_module)
        self._tmp.cleanup()

    def _enqueue(self, message: str, reason: str, queue: str = "order_queue") -> int:
        return int(self.svc.enqueue_dead_letter(queue, message, reason)["dlq_id"])

    def _fail_replay(self, dlq_id: int, reason: str = "重放超时") -> None:
        result = self.svc.batch_replay([dlq_id], replay_reason=reason, force_fail=True)
        self.assertEqual(result["failed"], 1)


class TestLegacyReasonParsing(DlqReasonTestBase):
    """历史上合在一个字段里的原因兼容。"""

    def test_single_field_with_replay_marker(self):
        events = parse_combined_reasons("原始格式有误 [重放重试]")
        self.assertEqual([event["phase"] for event in events], [PHASE_ORIGINAL, PHASE_REPLAY])
        self.assertEqual(events[0]["reason"], "原始格式有误")
        self.assertEqual(events[1]["reason"], "重放执行失败")

    def test_multi_segment_combined_field(self):
        events = parse_combined_reasons("原始格式有误;重放超时")
        self.assertEqual(events[0]["phase"], PHASE_ORIGINAL)
        self.assertEqual(events[0]["reason"], "原始格式有误")
        self.assertEqual(events[1]["phase"], PHASE_REPLAY)
        self.assertEqual(events[1]["reason"], "重放超时")

    def test_legacy_record_normalized_on_read(self):
        # 直接构造一条只有拼接字段、没有结构化事件的历史记录
        self.store.insert("dlq_handler", {
            "记录编号": "DLQ-LEGACY-1",
            "原始队列": "legacy_queue",
            "失败原因": "原始格式有误 [重放重试]",
            "原始消息体": "legacy",
            "重试次数": "2",
            "入队时间": "2026-09-01T00:00:00+00:00",
            "处置状态": "待处置",
            "重放批次": "",
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "_fingerprint": "legacy-fp-1",
        })
        items, total, _ = self.svc.list_entries(filters={"失败阶段": "重放失败"})
        self.assertEqual(total, 1)
        self.assertEqual(items[0]["重放失败原因"], "重放执行失败")
        self.assertEqual(items[0]["原失败原因"], "原始格式有误")
        # 原失败阶段也能筛到同一条
        _, total_original, _ = self.svc.list_entries(filters={"失败阶段": "原失败"})
        self.assertEqual(total_original, 1)


class TestReasonFiltering(DlqReasonTestBase):
    """原因明细筛选与消息级去重。"""

    def test_original_and_replay_failures_both_filterable(self):
        dlq_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(dlq_id, "重放超时")

        # 格式类（原失败）与超时类（重放失败）都能定位到同一条消息
        format_items, format_total, _ = self.svc.list_entries(filters={"原因分类": "格式类"})
        timeout_items, timeout_total, _ = self.svc.list_entries(filters={"原因分类": "超时类"})
        self.assertEqual(format_total, 1)
        self.assertEqual(timeout_total, 1)
        self.assertEqual(format_items[0]["id"], timeout_items[0]["id"])

        # 按阶段筛选
        _, replay_total, _ = self.svc.list_entries(filters={"失败阶段": "重放失败"})
        self.assertEqual(replay_total, 1)
        _, original_total, _ = self.svc.list_entries(filters={"失败阶段": "原失败"})
        self.assertEqual(original_total, 1)

        # 关键词同时匹配两个字段里的原因
        _, kw_total, _ = self.svc.list_entries(filters={"失败原因": "超时"})
        self.assertEqual(kw_total, 1)

    def test_concurrent_appended_reason_keeps_single_message(self):
        message = '{"id":42,"v":"same"}'
        first = self._enqueue(message, "原始格式有误")
        # 同一消息再次失败入队：原因以事件追加，不新增记录
        again = self.svc.enqueue_dead_letter("order_queue", message, "重复投递失败")
        self.assertTrue(again["is_duplicate"])
        self.assertEqual(again["dlq_id"], first)

        items, total, _ = self.svc.list_entries(filters={}, size=50)
        self.assertEqual(total, 1)
        self.assertEqual(len(items), 1)
        record = self.store.get_by_id("dlq_handler", first)
        self.assertEqual(len(record["_failure_events"]), 2)
        # 追加后按新原因也能查到这唯一一条消息
        _, kw_total, _ = self.svc.list_entries(filters={"失败原因": "重复投递"})
        self.assertEqual(kw_total, 1)

    def test_multiple_messages_counted_once_per_message(self):
        for i in range(5):
            dlq_id = self._enqueue(f'{{"id":{i}}}', "原始格式有误", queue=f"queue_{i % 2}")
            if i % 2 == 0:
                self._fail_replay(dlq_id)
        items, total, _ = self.svc.list_entries(filters={}, size=50)
        self.assertEqual(total, 5)
        self.assertEqual(len({item["id"] for item in items}), 5)
        # 同时带两类原因的消息，在合并分类筛选下仍只出现一次
        _, merged_total, _ = self.svc.list_entries(filters={}, size=50)
        self.assertEqual(merged_total, 5)


class TestCursorPagination(DlqReasonTestBase):
    """分页边界绑定原死信身份与筛选条件。"""

    def test_cursor_walks_all_pages_without_duplicate(self):
        for i in range(5):
            self._enqueue(f'{{"id":{i}}}', "重放超时" if i % 2 else "原始格式有误")

        page1, total, cursor = self.svc.list_entries(filters={}, size=2)
        self.assertEqual(total, 5)
        self.assertEqual(len(page1), 2)
        self.assertIsNotNone(cursor)
        payload = decode_cursor(cursor)
        self.assertEqual(payload["id"], page1[-1]["id"])
        self.assertIn("filter", payload)

        page2, _, cursor2 = self.svc.list_entries(filters={}, cursor=cursor, size=2)
        self.assertEqual(len(page2), 2)
        page3, _, cursor3 = self.svc.list_entries(filters={}, cursor=cursor2, size=2)
        self.assertEqual(len(page3), 1)
        self.assertIsNone(cursor3)

        seen = [row["id"] for row in page1 + page2 + page3]
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(len(seen), 5)

    def test_cursor_rejects_changed_filter(self):
        # 造足量数据，保证首页满页并产生游标
        for i in range(3):
            self._enqueue(f'{{"id":{i}}}', "原始格式有误")
        _, _, cursor = self.svc.list_entries(filters={"失败原因": "格式"}, size=2)
        self.assertIsNotNone(cursor)
        # 用同一游标换一个筛选条件，必须被拒绝而不是悄悄返回错误口径的数据
        with self.assertRaises(ValueError):
            self.svc.list_entries(filters={"失败原因": "超时"}, cursor=cursor)
        # 换原因分类同样被拒绝
        with self.assertRaises(ValueError):
            self.svc.list_entries(filters={"原因分类": "超时类"}, cursor=cursor)

    def test_cursor_bound_to_dlq_identity(self):
        for i in range(3):
            self._enqueue(f'{{"id":{i}}}', "重放超时")
        page1, _, cursor = self.svc.list_entries(filters={"原因分类": "超时类"}, size=2)
        payload = decode_cursor(cursor)
        self.assertEqual(payload["id"], page1[-1]["id"])
        self.assertEqual(payload["filter"]["category"], "超时类")


class TestBacklogLedger(DlqReasonTestBase):
    """积压台账与检索清单共用筛选规则，按原始消息计数。"""

    def test_backlog_total_counts_original_messages(self):
        for i in range(4):
            dlq_id = self._enqueue(f'{{"id":{i}}}', "原始格式有误", queue="order_queue")
            self._fail_replay(dlq_id)  # 每条消息带两个失败事件
        backlog = self.svc.get_backlog_summary({})
        # 4 条消息 × 2 个失败事件，积压总量仍是 4（按原始消息去重）
        self.assertEqual(backlog["积压消息总数"], 4)
        self.assertEqual(backlog["积压记录数"], 4)

    def test_backlog_shares_filter_with_list(self):
        id_format = self._enqueue('{"id":1}', "原始格式有误")
        id_timeout = self._enqueue('{"id":2}', "处理超时")
        self._fail_replay(id_timeout, "重放超时")
        self.svc.run_action(id_format, "标记丢弃")

        filters = {"原因分类": "格式类"}
        _, list_total, _ = self.svc.list_entries(filters=filters)
        backlog = self.svc.get_backlog_summary(filters)
        # 格式类消息已丢弃，不再计入积压；检索清单仍能回查到它
        self.assertEqual(list_total, 1)
        self.assertEqual(backlog["积压消息总数"], 0)

        timeout_backlog = self.svc.get_backlog_summary({"原因分类": "超时类"})
        self.assertEqual(timeout_backlog["积压消息总数"], 1)
        groups = {(g["失败阶段"], g["失败原因"]) for g in timeout_backlog["原因分组"]}
        self.assertIn((PHASE_ORIGINAL, "处理超时"), groups)
        self.assertIn((PHASE_REPLAY, "重放超时"), groups)
        # 每个分组对同一消息只计一次
        for group in timeout_backlog["原因分组"]:
            self.assertEqual(group["消息数"], 1)

    def test_backlog_by_source_queue(self):
        self._enqueue('{"id":1}', "处理超时", queue="queue_a")
        self._enqueue('{"id":2}', "处理超时", queue="queue_a")
        self._enqueue('{"id":3}', "处理超时", queue="queue_b")
        backlog = self.svc.get_backlog_summary({})
        self.assertEqual(backlog["按原始队列"], {"queue_a": 2, "queue_b": 1})


class TestReasonUsableScope(DlqReasonTestBase):
    """区分可用于继续处置的原因与只供回查的原因。"""

    def test_action_vs_reference_scope(self):
        pending_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(pending_id)
        terminal_id = self._enqueue('{"id":2}', "认证失败")
        self.svc.run_action(terminal_id, "标记丢弃")

        _, action_total, _ = self.svc.list_entries(filters={"原因口径": USABLE_ACTION})
        _, reference_total, _ = self.svc.list_entries(filters={"原因口径": USABLE_REFERENCE})
        self.assertEqual(action_total, 1)
        self.assertEqual(reference_total, 1)

        options = self.svc.get_reason_options({})
        by_reason = {option["失败原因"]: option for option in options}
        self.assertEqual(by_reason["原始格式有误"]["原因口径"], USABLE_ACTION)
        self.assertEqual(by_reason["认证失败"]["原因口径"], USABLE_REFERENCE)
        self.assertEqual(by_reason["原始格式有误"]["可处置消息数"], 1)
        self.assertEqual(by_reason["认证失败"]["回查消息数"], 1)

    def test_scope_switches_after_status_transition(self):
        dlq_id = self._enqueue('{"id":1}', "处理超时")
        _, before, _ = self.svc.list_entries(filters={"原因口径": USABLE_ACTION})
        self.assertEqual(before, 1)
        self.svc.run_action(dlq_id, "标记重放")
        _, action_after, _ = self.svc.list_entries(filters={"原因口径": USABLE_ACTION})
        _, reference_after, _ = self.svc.list_entries(filters={"原因口径": USABLE_REFERENCE})
        self.assertEqual(action_after, 0)
        self.assertEqual(reference_after, 1)


class TestReviewLedger(DlqReasonTestBase):
    """值班复核台账：班组复核、签收与口径回查。"""

    def test_two_teams_review_with_own_filters(self):
        msg_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(msg_id, "重放超时")
        self._enqueue('{"id":2}', "认证失败")

        review_a = self.svc.create_review("一班", {"原因分类": "格式类"})
        review_b = self.svc.create_review("二班", {"原因分类": "认证类"})
        self.assertNotEqual(review_a["id"], review_b["id"])
        self.assertEqual(review_a["原因口径"], USABLE_ACTION)
        self.assertEqual(review_a["积压消息总数"], 1)
        self.assertEqual(review_b["积压消息总数"], 1)
        # 分组快照落账
        self.assertTrue(any(g["原因分类"] == "格式类" for g in review_a["原因分组"]))

    def test_signed_review_relocates_filter_and_both_failures(self):
        msg_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(msg_id, "重放超时")
        review = self.svc.create_review("值班班组", {"失败阶段": "重放失败"})
        signed, message = self.svc.sign_review(review["id"], "张值班")
        self.assertTrue(message)
        self.assertEqual(signed["复核状态"], "已签收")
        self.assertEqual(signed["签收人"], "张值班")

        detail = self.svc.get_review(review["id"])
        # 当时的筛选口径
        self.assertEqual(detail["筛选口径"]["phase"], "重放失败")
        snapshot = detail["消息快照"][0]
        self.assertEqual(snapshot["原失败原因"], "原始格式有误")
        self.assertEqual(snapshot["重放失败原因"], "重放超时")
        self.assertEqual(len(snapshot["失败原因明细"]), 2)

        # 不可重复签收
        again, again_message = self.svc.sign_review(review["id"], "李值班")
        self.assertIsNone(again)
        self.assertIn("已签收", again_message)

    def test_review_list_and_filter(self):
        review = self.svc.create_review("班组", {})
        self.svc.sign_review(review["id"], "签收人")
        pending = self.svc.list_reviews(status="待签收")
        signed = self.svc.list_reviews(status="已签收")
        self.assertEqual(len(pending), 0)
        self.assertEqual(len(signed), 1)

    def test_reference_scope_review_keeps_terminal_records(self):
        dlq_id = self._enqueue('{"id":1}', "原始格式有误")
        self.svc.run_action(dlq_id, "标记丢弃")
        review = self.svc.create_review("回查班组", {"原因口径": USABLE_REFERENCE})
        self.assertEqual(review["原因口径"], USABLE_REFERENCE)
        self.assertEqual(review["积压消息总数"], 1)


class TestMonitorCrossModule(DlqReasonTestBase):
    """集成监控跨模块使用死信原因筛选数据。"""

    def test_monitor_reuses_dlq_reason_data(self):
        dlq_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(dlq_id, "重放超时")
        monitor = monitor_module.IntegrationMonitorService()
        dashboard = monitor.get_dlq_reason_dashboard({"原因分类": "超时类"})
        self.assertEqual(dashboard["backlog"]["积压消息总数"], 1)
        self.assertTrue(any(g["失败原因"] == "重放超时" for g in dashboard["reason_options"]))

        metrics = monitor.collect_global_metrics()
        self.assertIn("reason_options", metrics["dlq"])
        self.assertEqual(metrics["dlq"]["backlog_total"], 1)

    def test_monitor_writes_review_ledger(self):
        self._enqueue('{"id":1}', "处理超时")
        monitor = monitor_module.IntegrationMonitorService()
        review = monitor.export_dlq_review_ledger("监控值班", {"原因分类": "超时类"})
        self.assertEqual(review["复核状态"], "待签收")
        self.assertTrue(self.store.get_by_id(dlq_module.REVIEW_LEDGER_KEY, review["id"]))


class TestFailureAnalysis(DlqReasonTestBase):
    """原因分析在明细模型下保持可用。"""

    def test_analysis_counts_events_and_messages(self):
        dlq_id = self._enqueue('{"id":1}', "原始格式有误")
        self._fail_replay(dlq_id, "重放超时")
        self._enqueue('{"id":2}', "处理超时")
        analysis = self.svc.analyze_failure_reasons()
        self.assertEqual(analysis["total"], 2)  # 消息维度
        reason_names = [name for name, _ in analysis["top_reasons"]]
        self.assertIn("重放超时", reason_names)
        self.assertIn("原始格式有误", reason_names)
        self.assertIn("超时类", analysis["categories"])
        self.assertIn("格式类", analysis["categories"])
        self.assertEqual(analysis["pending_count"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
