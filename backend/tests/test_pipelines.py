"""跨模块数据流管道单元测试与集成测试。

测试以下真实数据链路：
1. Webhook接收 → 事件解析 → 去重 → 分发路由 → 数据落湖
2. 死信入队 → 原因分析 → 批量重放 → 追踪
3. 批量回补 → 分页拉取 → 数据落湖
4. 去重逻辑（幂等键+内容指纹+时间窗口）
5. 增量水位标记与断点恢复
6. 数据转换 → 质量校验完整链路
7. 端到端监控指标聚合
"""
from __future__ import annotations

import json
import os
import sys
import unittest

# 将backend目录加入路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import json_store
from app.services.api_endpoint import ApiEndpointService
from app.services.webhook_channel import WebhookChannelService
from app.services.event_schema import EventSchemaService
from app.services.mq_consumer import MqConsumerService
from app.services.dlq_handler import DlqHandlerService
from app.services.api_auth_store import ApiAuthStoreService
from app.services.rate_limit_config import RateLimitConfigService
from app.services.data_transform import DataTransformService
from app.services.dispatch_rule import DispatchRuleService
from app.services.dedup_filter import DedupFilterService
from app.services.batch_ingest import BatchIngestService
from app.services.data_lake_sink import DataLakeSinkService
from app.services.data_quality_rule import DataQualityRuleService
from app.services.call_trace_log import CallTraceLogService
from app.services.incre_cursor import IncreCursorService
from app.services.backfill_job import BackfillJobService
from app.services.event_archive import EventArchiveService
from app.services.integration_monitor import IntegrationMonitorService


class TestDataPersistence(unittest.TestCase):
    """测试数据持久化层。"""

    def setUp(self):
        self.test_key = "test_module"
        json_store.truncate(self.test_key)

    def test_insert_and_query(self):
        record = {"name": "test_record", "value": 100}
        result = json_store.insert(self.test_key, record)
        self.assertEqual(result["name"], "test_record")
        self.assertIn("id", result)

        all_records = json_store.get_all(self.test_key)
        self.assertEqual(len(all_records), 1)

    def test_update_and_delete(self):
        record = json_store.insert(self.test_key, {"name": "to_update"})
        updated = json_store.update(self.test_key, record["id"], {"name": "updated_value"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["name"], "updated_value")

        deleted = json_store.delete(self.test_key, record["id"])
        self.assertTrue(deleted)
        self.assertEqual(len(json_store.get_all(self.test_key)), 0)

    def test_query_with_filters(self):
        for i in range(20):
            json_store.insert(self.test_key, {"name": f"record_{i}", "status": "active" if i % 2 == 0 else "blocked"})

        items, total = json_store.query(self.test_key, filters={"status": "active"}, page=1, size=10)
        self.assertEqual(total, 10)
        self.assertLessEqual(len(items), 10)

    def test_aggregate(self):
        for name in ["A", "B", "A", "C", "B", "A"]:
            json_store.insert(self.test_key, {"name": name})

        counts = json_store.aggregate(self.test_key, "name")
        self.assertEqual(counts["A"], 3)
        self.assertEqual(counts["B"], 2)
        self.assertEqual(counts["C"], 1)


class TestWebhookToLakePipeline(unittest.TestCase):
    """链路1测试：Webhook接收 → 事件解析 → 去重 → 分发 → 落湖。"""

    def setUp(self):
        self.webhook_svc = WebhookChannelService()
        self.schema_svc = EventSchemaService()
        self.dedup_svc = DedupFilterService()
        json_store.truncate("webhook_channel")
        json_store.truncate("event_schema")

        # 创建测试通道
        self.channel = json_store.insert("webhook_channel", {
            "通道编号": "WEBH-TEST-001",
            "通道名称": "测试通道",
            "接收路径": "/webhook/test",
            "签名密钥": "test_secret_12345",
            "验证方式": "NONE",
            "并发上限": "10",
            "回调地址": "",
            "通道版本": "v1",
            "status": "已启用",
            "pending": False,
            "abnormal": False,
        })
        self.channel_id = self.channel["id"]

    def test_channel_not_found(self):
        result = self.webhook_svc.receive_webhook(channel_id=99999, payload={"type": "test", "data": {}}, signature=None)
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "通道校验")

    def test_channel_disabled(self):
        json_store.update("webhook_channel", self.channel_id, {"status": "草稿"})
        result = self.webhook_svc.receive_webhook(channel_id=self.channel_id, payload={"type": "test", "data": {}}, signature=None)
        self.assertFalse(result["ok"])
        self.assertIn("状态校验", result.get("stage", ""))

    def test_webhook_receive_full_chain(self):
        payload = {"type": "order_create", "data": {"id": 123, "amount": 99.9, "customer": "test_user"}}
        result = self.webhook_svc.receive_webhook(
            channel_id=self.channel_id,
            payload=payload,
            signature=None,
        )
        # 通道已启用，无签名验证要求，不检查schema（无匹配schema）-> 应该成功
        self.assertTrue(result["ok"], f"Expected ok=True, got: {result.get('message')}")
        self.assertEqual(result["stage"], "完成")
        self.assertIsNotNone(result.get("event_id"))

    def test_webhook_channel_stats(self):
        for i in range(3):
            self.webhook_svc.receive_webhook(
                channel_id=self.channel_id,
                payload={"type": "test", "data": {"seq": i}},
                signature=None,
            )
        stats = self.webhook_svc.get_channel_stats(self.channel_id)
        self.assertTrue(stats["ok"])
        self.assertGreaterEqual(stats["success_count"], 1)


class TestDlqReplay(unittest.TestCase):
    """链路2测试：死信入队 → 分析 → 批量重放。"""

    def setUp(self):
        self.dlq_svc = DlqHandlerService()
        json_store.truncate("dlq_handler")

    def test_enqueue_dead_letter(self):
        result = self.dlq_svc.enqueue_dead_letter(
            source_queue="order_queue",
            original_message='{"id": 123, "type": "order"}',
            failure_reason="处理超时",
            consumer_id=1,
        )
        self.assertTrue(result["ok"])
        self.assertIn("dlq_id", result)
        self.assertFalse(result.get("is_duplicate"))
        self.assertIn("DLQ-", result.get("message", ""))

    def test_duplicate_dlq_detection(self):
        first = self.dlq_svc.enqueue_dead_letter("order_queue", '{"id": 456}', "格式异常", 1)
        second = self.dlq_svc.enqueue_dead_letter("order_queue", '{"id": 456}', "格式异常", 1)
        self.assertTrue(second["ok"])
        self.assertTrue(second.get("is_duplicate"))
        self.assertEqual(second["dlq_id"], first["dlq_id"])

    def test_failure_analysis(self):
        reasons = ["处理超时", "数据格式异常", "处理超时", "网络连接失败", "认证失败"]
        for i, reason in enumerate(reasons):
            # 消息体需唯一，否则同指纹消息会按原始消息合并为一条死信记录
            self.dlq_svc.enqueue_dead_letter(
                f"queue_{i % 2}", f'{{"id": {i}, "seq": {reason}-{i}}}', reason, 1)

        analysis = self.dlq_svc.analyze_failure_reasons()
        self.assertEqual(analysis["total"], 5)
        self.assertGreater(len(analysis["categories"]), 0)
        self.assertIn("处理超时", [r[0] for r in analysis["top_reasons"]])

    def test_batch_replay(self):
        dlq_ids = []
        for i in range(5):
            r = self.dlq_svc.enqueue_dead_letter("test_queue", f'{{"id": {i}}}', "测试失败原因", i)
            dlq_ids.append(r["dlq_id"])

        result = self.dlq_svc.batch_replay(dlq_ids)
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["replayed"] + result["failed"], 1)
        self.assertEqual(result["total"], 5)


class TestBatchBackfill(unittest.TestCase):
    """链路3测试：批量接入调度 → 分页拉取 → 数据落湖。"""

    def setUp(self):
        self.batch_svc = BatchIngestService()
        self.backfill_svc = BackfillJobService()
        json_store.truncate("batch_ingest")
        json_store.truncate("backfill_job")

        # 创建测试接入任务
        self.task = json_store.insert("batch_ingest", {
            "任务编号": "BATCH-TEST-001",
            "关联端点": "1",
            "Cron表达式": "* * * * *",
            "分页策略": "逐页",
            "并行批次": "2",
            "断点续传": "是",
            "失败动作": "记录并继续",
            "任务版本": "v1",
            "status": "运行中",
            "pending": False,
            "abnormal": False,
        })

        # 创建测试回补任务
        self.job = json_store.insert("backfill_job", {
            "任务编号": "BACK-TEST-001",
            "关联端点": "1",
            "起始日期": "2026-11-01",
            "截止日期": "2026-11-05",
            "并行度": "2",
            "覆盖策略": "追加不覆写",
            "完成水位": "0/5",
            "失败区间": "无",
            "status": "运行中",
            "pending": False,
            "abnormal": False,
        })

    def test_batch_execute(self):
        result = self.batch_svc.execute_batch(task_id=self.task["id"], max_pages=3)
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["pages_processed"], 1)
        self.assertGreaterEqual(result["records_fetched"], 0)

    def test_batch_not_running(self):
        json_store.update("batch_ingest", self.task["id"], {"status": "草稿"})
        result = self.batch_svc.execute_batch(task_id=self.task["id"])
        self.assertFalse(result["ok"])

    def test_backfill_execute(self):
        result = self.backfill_svc.execute_backfill(job_id=self.job["id"])
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["days_processed"], 1)
        self.assertGreaterEqual(result["records_backfilled"], 1)

    def test_backfill_progress(self):
        self.backfill_svc.execute_backfill(job_id=self.job["id"])
        progress = self.backfill_svc.get_backfill_progress(self.job["id"])
        self.assertTrue(progress["ok"])
        self.assertGreater(progress["progress_pct"], 0)


class TestDedupLogic(unittest.TestCase):
    """链路4测试：去重逻辑（幂等键、内容指纹、时间窗口）。"""

    def setUp(self):
        self.dedup_svc = DedupFilterService()
        json_store.truncate("dedup_filter")
        json_store.insert("dedup_filter", {
            "规则编号": "DEDUP-TEST-001",
            "事件类型": "通用",
            "去重键列": "id",
            "比较方式": "精确匹配",
            "时间窗口": "24小时",
            "相似度阈值": "0.95",
            "冲突策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_first_event_not_duplicate(self):
        result = self.dedup_svc.check_duplicate("order_create", {"id": "order_001", "amount": 100})
        self.assertFalse(result["is_duplicate"])
        self.assertEqual(result["reason"], "通过去重检查")

    def test_duplicate_event_detected(self):
        payload = {"id": "order_002", "amount": 200}
        first = self.dedup_svc.check_duplicate("order_create", payload)
        self.assertFalse(first["is_duplicate"])
        second = self.dedup_svc.check_duplicate("order_create", payload)
        self.assertTrue(second["is_duplicate"])

    def test_deduplication_stats(self):
        for i in range(5):
            self.dedup_svc.check_duplicate("test_event", {"id": f"evt_{i}", "value": i})
        # 重复发送前2个
        for i in range(2):
            self.dedup_svc.check_duplicate("test_event", {"id": f"evt_{i}", "value": i})

        stats = self.dedup_svc.get_dedup_stats()
        self.assertEqual(stats["total_duplicates"], 2)
        self.assertIsInstance(stats["total_idempotency_keys"], int)
        self.assertGreater(stats["total_idempotency_keys"], 0)

    def test_clear_cache(self):
        for i in range(3):
            self.dedup_svc.check_duplicate("test", {"id": f"evt_{i}"})
        self.dedup_svc.clear_dedup_cache()
        stats = self.dedup_svc.get_dedup_stats()
        self.assertEqual(stats["total_idempotency_keys"], 0)


class TestIncreCursor(unittest.TestCase):
    """链路5测试：增量水位标记与断点恢复。"""

    def setUp(self):
        self.cursor_svc = IncreCursorService()
        json_store.truncate("incre_cursor")

    def test_update_position(self):
        result = self.cursor_svc.update_position(endpoint_id=1, new_position=100, data_count=50)
        self.assertTrue(result["ok"])
        self.assertEqual(result["new_position"], 100)
        self.assertEqual(result["delta"], 100)

    def test_incremental_update(self):
        self.cursor_svc.update_position(endpoint_id=2, new_position=50)
        self.cursor_svc.update_position(endpoint_id=2, new_position=150)
        pos = self.cursor_svc.get_position(2)
        self.assertEqual(pos["position"], 150)

    def test_reset_position(self):
        self.cursor_svc.update_position(endpoint_id=3, new_position=200)
        result = self.cursor_svc.reset_position(endpoint_id=3, reset_to=0)
        self.assertTrue(result["ok"])
        pos = self.cursor_svc.get_position(3)
        self.assertEqual(pos["position"], 0)

    def test_position_history(self):
        for i in range(3):
            self.cursor_svc.update_position(endpoint_id=4, new_position=(i + 1) * 100)
        history = self.cursor_svc.get_position_history(4)
        self.assertGreaterEqual(len(history), 3)


class TestDataTransformQuality(unittest.TestCase):
    """链路6测试：数据转换 → 质量校验。"""

    def setUp(self):
        self.transform_svc = DataTransformService()
        self.quality_svc = DataQualityRuleService()
        json_store.truncate("data_transform")
        json_store.truncate("data_quality_rule")

        self.transform_rule = json_store.insert("data_transform", {
            "规则编号": "TRANS-TEST-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": '{"target_id": "id", "target_name": "name", "target_value": "value"}',
            "转换函数": "passthrough",
            "默认值填充": "使用默认值",
            "异常策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

        self.quality_rule = json_store.insert("data_quality_rule", {
            "规则编号": "QUAL-TEST-001",
            "数据来源": "ALL",
            "校验字段": "name",
            "校验表达式": "not_empty",
            "阈值下界": "0",
            "阈值上界": "0",
            "阻断策略": "警告",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_apply_transform(self):
        source = {"id": 1001, "name": "Test Record", "value": 42.5, "extra": "ignored"}
        result = self.transform_svc.apply_transform(rule_id=self.transform_rule["id"], source_data=source)
        self.assertTrue(result["ok"])
        self.assertIn("target_id", result["data"])
        self.assertEqual(result["data"]["target_name"], "Test Record")
        self.assertGreaterEqual(result["fields_transformed"], 1)

    def test_transform_rule_not_found(self):
        result = self.transform_svc.apply_transform(rule_id=99999, source_data={"id": 1})
        self.assertFalse(result["ok"])

    def test_quality_validation(self):
        valid_record = {"name": "Valid Record", "value": 100}
        result = self.quality_svc.validate_record(valid_record, source_module="test")
        self.assertTrue(result["passed"])
        self.assertGreaterEqual(result["score"], 90)

    def test_quality_empty_field(self):
        empty_record = {"name": "", "value": 0}
        result = self.quality_svc.validate_record(empty_record, source_module="test")
        self.assertFalse(result["passed"])

    def test_batch_validation(self):
        records = [
            {"name": "Valid1", "value": 100},
            {"name": "", "value": 200},
            {"name": "Valid2", "value": 300},
        ]
        result = self.quality_svc.validate_batch(records, "test")
        self.assertGreaterEqual(result["passed"], 1)
        self.assertGreaterEqual(result["failed"], 1)


class TestCredentialLifecycle(unittest.TestCase):
    """测试凭证生成、验证与轮换检测。"""

    def setUp(self):
        self.auth_svc = ApiAuthStoreService()
        self.endpoint_svc = ApiEndpointService()
        json_store.truncate("api_auth_store")
        json_store.truncate("api_endpoint")

        self.endpoint = json_store.insert("api_endpoint", {
            "端点编号": "EP-TEST-001",
            "端点名称": "测试API",
            "请求方法": "GET",
            "基础URL": "http://test-api.example.com",
            "认证类型": "Bearer",
            "超时秒数": "30",
            "重试次数": "3",
            "端点版本": "v1",
            "status": "已接入",
            "pending": False,
            "abnormal": False,
        })

    def test_generate_credential(self):
        result = self.auth_svc.generate_credential(
            endpoint_id=self.endpoint["id"],
            cred_type="Bearer",
            validity_days=7,
        )
        self.assertTrue(result["ok"])
        self.assertIn("credential_id", result)
        self.assertIn("api_key", result)
        self.assertEqual(result["credential_type"], "Bearer")

    def test_verify_valid_credential(self):
        cred = self.auth_svc.generate_credential(endpoint_id=self.endpoint["id"], cred_type="APIKey", validity_days=7)
        result = self.auth_svc.verify_credential(credential_id=cred["credential_id"], api_key=cred["api_key"])
        self.assertTrue(result["ok"])

    def test_verify_invalid_key(self):
        cred = self.auth_svc.generate_credential(endpoint_id=self.endpoint["id"], cred_type="APIKey")
        result = self.auth_svc.verify_credential(credential_id=cred["credential_id"], api_key="wrong_key")
        self.assertFalse(result["ok"])
        self.assertEqual(result["message"], "密钥验证失败")

    def test_pending_rotation_check(self):
        result = self.auth_svc.auto_rotation_check()
        self.assertIsInstance(result, list)


class TestMonitoringHealth(unittest.TestCase):
    """链路7测试：全局监控指标聚合。"""

    def setUp(self):
        self.monitor_svc = IntegrationMonitorService()
        json_store.truncate("integration_monitor")

    def test_collect_metrics(self):
        metrics = self.monitor_svc.collect_global_metrics()
        self.assertIn("health_score", metrics)
        self.assertIn("health_status", metrics)
        self.assertIn("trace", metrics)
        self.assertIn("quality", metrics)
        self.assertIn("dlq", metrics)

    def test_health_status(self):
        health = self.monitor_svc.get_health_status()
        self.assertIn("health_score", health)
        self.assertIn("health_status", health)
        self.assertGreaterEqual(health["health_score"], 0)
        self.assertLessEqual(health["health_score"], 100)

    def test_alerts_retrieval(self):
        # 先收集一次指标触发告警检查
        self.monitor_svc.collect_global_metrics()
        alerts = self.monitor_svc.get_alerts(limit=5)
        self.assertIsInstance(alerts, list)


class TestRateLimit(unittest.TestCase):
    """测试多级限流检查。"""

    def setUp(self):
        self.rate_svc = RateLimitConfigService()
        json_store.truncate("rate_limit_config")
        json_store.insert("rate_limit_config", {
            "规则编号": "RATE-TEST-001",
            "关联端点": "1",
            "请求间隔": "10",
            "并发上限": "5",
            "日配额": "1000",
            "月配额": "30000",
            "突发容量": "20",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_first_request_allowed(self):
        result = self.rate_svc.check_limit(endpoint_id=1)
        self.assertTrue(result["allowed"])
        self.assertGreaterEqual(result["rules_checked"], 1)

    def test_rate_metrics(self):
        for _ in range(5):
            self.rate_svc.check_limit(endpoint_id=1)
        metrics = self.rate_svc.get_rate_limit_metrics(endpoint_id=1)
        self.assertGreaterEqual(metrics["total_daily_used"], 5)
        self.assertEqual(metrics["rules"], 1)


class TestDispatchRules(unittest.TestCase):
    """测试事件分发路由。"""

    def setUp(self):
        self.dispatch_svc = DispatchRuleService()
        json_store.truncate("dispatch_rule")
        json_store.insert("dispatch_rule", {
            "规则编号": "DISP-TEST-001",
            "事件类型": "order_create",
            "目标系统": "order_system",
            "分发方式": "异步",
            "过滤条件": "",
            "优先级": "高",
            "失败动作": "重试3次",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        json_store.insert("dispatch_rule", {
            "规则编号": "DISP-TEST-002",
            "事件类型": "通用",
            "目标系统": "default_sink",
            "分发方式": "同步",
            "过滤条件": "",
            "优先级": "低",
            "失败动作": "记录日志",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_match_specific_event(self):
        targets = self.dispatch_svc.match_targets("order_create", {"data": {"id": 1}})
        self.assertGreaterEqual(len(targets), 1)
        self.assertEqual(targets[0]["目标系统"], "order_system")

    def test_default_match_for_unknown_event(self):
        targets = self.dispatch_svc.match_targets("unknown_event", {"data": {}})
        self.assertGreaterEqual(len(targets), 1)
        self.assertEqual(targets[0]["目标系统"], "default_sink")

    def test_dispatch_plan(self):
        plan = self.dispatch_svc.get_dispatch_plan("order_create")
        self.assertGreaterEqual(len(plan), 1)


class TestEventArchive(unittest.TestCase):
    """测试事件归档清理。"""

    def setUp(self):
        self.archive_svc = EventArchiveService()
        json_store.truncate("event_archive")
        json_store.insert("event_archive", {
            "策略编号": "ARCH-TEST-001",
            "事件类型": "ALL",
            "归档条件": "days>0",
            "归档路径": "/archive",
            "压缩格式": "zlib",
            "保留天数": "90",
            "清理方式": "删除源文件",
            "恢复超时": "300",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_archive_stats(self):
        stats = self.archive_svc.get_archive_stats()
        self.assertIsInstance(stats, dict)
        self.assertIn("total_files", stats)

    def test_execute_archive(self):
        # 创建一条已生效的归档策略
        result = json_store.get_all("event_archive")
        policy_ids = [r["id"] for r in result if r.get("status") == "已生效"]
        if policy_ids:
            result = self.archive_svc.execute_archive(policy_id=policy_ids[0])
            self.assertTrue(result["ok"])


class TestMqConsumer(unittest.TestCase):
    """测试MQ消费者批量消费。"""

    def setUp(self):
        self.consumer_svc = MqConsumerService()
        json_store.truncate("mq_consumer")
        self.consumer = json_store.insert("mq_consumer", {
            "配置编号": "MQ-TEST-001",
            "队列名称": "test_queue",
            "消费组名": "test_group",
            "偏移策略": "earliest",
            "批量大小": "5",
            "超时时间": str(5000),
            "死信队列": "test_dlq",
            "消费状态": "消费中",
            "status": "消费中",
            "pending": False,
            "abnormal": False,
        })

    def test_consume_batch(self):
        result = self.consumer_svc.consume_batch(consumer_id=self.consumer["id"], max_messages=5)
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["consumed"] + result["failed"], 1)

    def test_consumer_state(self):
        state = self.consumer_svc.get_consumer_state(self.consumer["id"])
        self.assertTrue(state["ok"])
        self.assertEqual(state["status"], "消费中")

    def test_offset_reset(self):
        # 先停止消费者
        json_store.update("mq_consumer", self.consumer["id"], {"status": "已停止"})
        result = self.consumer_svc.reset_offset(consumer_id=self.consumer["id"], target_offset=50)
        self.assertTrue(result["ok"])


class TestDomainSnapshot(unittest.TestCase):
    """测试所有领域服务的domain_snapshot输出。"""

    services = [
        ApiEndpointService(),
        WebhookChannelService(),
        EventSchemaService(),
        MqConsumerService(),
        DlqHandlerService(),
        ApiAuthStoreService(),
        RateLimitConfigService(),
        DataTransformService(),
        DispatchRuleService(),
        DedupFilterService(),
        BatchIngestService(),
        DataLakeSinkService(),
        DataQualityRuleService(),
        CallTraceLogService(),
        IncreCursorService(),
        BackfillJobService(),
        EventArchiveService(),
        IntegrationMonitorService(),
    ]

    def test_all_snapshots(self):
        for svc in self.services:
            with self.subTest(module=svc.MODULE):
                snapshot = svc.domain_snapshot()
                self.assertIn("module", snapshot)
                self.assertIn("count", snapshot)
                self.assertIn("artifact", snapshot)
                self.assertIn("operation", snapshot)
                self.assertIn("by_status", snapshot)
                self.assertEqual(snapshot["module"], svc.MODULE)


if __name__ == "__main__":
    unittest.main(verbosity=2)