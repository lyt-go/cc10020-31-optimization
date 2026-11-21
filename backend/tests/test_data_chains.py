"""cc10020 业务链路测试：验证跨模块数据流的正确性。

测试覆盖6条关键跨模块链路：
1. webhook接收 → 事件解析 → 分发路由 → 落湖存储
2. API批量拉取 → 数据转换 → 质量校验 → 落湖
3. 死信队列 → 分析 → 重放 → 正常流程
4. 批量回补 → 增量水位 → 落湖 → 监控
5. 事件接收 → 去重过滤 → 分发 → 归档
6. 调用追踪 → 监控统计 → 质量评分
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore, json_store as global_store
from app.services.api_endpoint import ApiEndpointService
from app.services.webhook_channel import WebhookChannelService
from app.services.event_schema import EventSchemaService
from app.services.mq_consumer import MqConsumerService
from app.services.dlq_handler import DlqHandlerService
from app.services.data_lake_sink import DataLakeSinkService
from app.services.data_transform import DataTransformService
from app.services.data_quality_rule import DataQualityRuleService
from app.services.dispatch_rule import DispatchRuleService
from app.services.dedup_filter import DedupFilterService
from app.services.batch_ingest import BatchIngestService
from app.services.incre_cursor import IncreCursorService
from app.services.backfill_job import BackfillJobService
from app.services.call_trace_log import CallTraceLogService
from app.services.rate_limit_config import RateLimitConfigService
from app.services.api_auth_store import ApiAuthStoreService
from app.services.event_archive import EventArchiveService
from app.services.integration_monitor import IntegrationMonitorService


@pytest.fixture()
def isolated_store():
    """每个测试前清空存储，确保隔离；仅被本模块测试显式引用。"""
    with tempfile.TemporaryDirectory() as tmp:
        store = JsonStore(base_dir=tmp)
        import app.persistence
        app.persistence.json_store = store

        import app.services.api_endpoint as api_mod
        import app.services.webhook_channel as wh_mod
        import app.services.event_schema as es_mod
        import app.services.mq_consumer as mq_mod
        import app.services.dlq_handler as dlq_mod
        import app.services.data_lake_sink as sink_mod
        import app.services.data_transform as dt_mod
        import app.services.data_quality_rule as dq_mod
        import app.services.dispatch_rule as dr_mod
        import app.services.dedup_filter as df_mod
        import app.services.batch_ingest as bi_mod
        import app.services.incre_cursor as ic_mod
        import app.services.backfill_job as bj_mod
        import app.services.call_trace_log as ctl_mod
        import app.services.rate_limit_config as rlc_mod
        import app.services.api_auth_store as aas_mod
        import app.services.event_archive as ea_mod
        import app.services.integration_monitor as im_mod

        # Re-import all services to pick up new store.
        # 基础/被依赖模块先 reload：base 先于领域服务，dlq_handler 先于 mq_consumer，
        # 否则懒加载会拿到 reload 前的旧模块绑定。
        import importlib
        import app.services.base as base_mod
        import app.services.dlq_reasons as dlq_reasons_mod
        for mod in [base_mod, dlq_reasons_mod, api_mod, wh_mod, es_mod, dlq_mod, mq_mod,
                     sink_mod, dt_mod, dq_mod, dr_mod, df_mod, bi_mod, ic_mod, bj_mod,
                     ctl_mod, rlc_mod, aas_mod, ea_mod, im_mod]:
            importlib.reload(mod)

        yield

        import app.persistence
        app.persistence.json_store = global_store
        # 服务模块在 reload 时把 json_store 名字绑定到了临时存储，
        # 恢复全局存储后需再次 reload，避免后续套件引用已销毁的临时目录。
        import importlib
        for mod in [base_mod, dlq_reasons_mod, api_mod, wh_mod, es_mod, dlq_mod, mq_mod,
                     sink_mod, dt_mod, dq_mod, dr_mod, df_mod, bi_mod, ic_mod, bj_mod,
                     ctl_mod, rlc_mod, aas_mod, ea_mod, im_mod]:
            importlib.reload(mod)


# ──────────────────────────────────────────────────────────
# 链路1: webhook接收 → 事件解析 → 分发路由 → 落湖存储
# ──────────────────────────────────────────────────────────
def test_chain_1_webhook_to_lake(isolated_store):
    """验证完整的webhook事件接收到数据落湖链路。"""
    webhook_svc = WebhookChannelService()
    schema_svc = EventSchemaService()
    dispatch_svc = DispatchRuleService()
    sink_svc = DataLakeSinkService()

    # 创建通道
    channel = webhook_svc.create_entry({
        "通道编号": "WH-001",
        "通道名称": "订单事件通道",
        "接收路径": "/webhook/order",
        "签名密钥": "secret123",
        "验证方式": "NONE",
        "并发上限": "100",
        "回调地址": "https://callback.example.com",
        "通道版本": "v1",
    })
    assert channel[0] is not None, f"通道创建失败: {channel[1]}"
    channel_id = channel[0]["id"]
    webhook_svc.run_action(channel_id, "启用通道")

    # 创建Schema
    schema_svc.create_entry({
        "结构编号": "SC-001",
        "事件类型": "order_created",
        "字段名称": "order_id",
        "字段类型": "string",
        "必填标识": "是",
        "默认值": "",
        "Schema版本": "v1",
        "所属通道": str(channel_id),
    })
    schema_svc.create_entry({
        "结构编号": "SC-002",
        "事件类型": "order_created",
        "字段名称": "amount",
        "字段类型": "number",
        "必填标识": "否",
        "默认值": "0",
        "Schema版本": "v1",
        "所属通道": str(channel_id),
    })
    schemas = app.persistence.json_store.get_all("event_schema")
    for s in schemas:
        schema_svc.run_action(int(s["id"]), "标记生效")

    # 创建分发规则
    dispatch_svc.create_entry({
        "规则编号": "DR-001",
        "事件类型": "order_created",
        "目标系统": "order_system",
        "分发方式": "异步",
        "过滤条件": "",
        "优先级": "高",
        "失败动作": "记录日志",
        "规则版本": "v1",
    })
    drs = app.persistence.json_store.get_all("dispatch_rule")
    for dr in drs:
        dispatch_svc.run_action(int(dr["id"]), "标记生效")

    # 发送Webhook事件
    result = webhook_svc.receive_webhook(channel_id, {
        "type": "order_created",
        "order_id": "ORD-2024-001",
        "amount": 299.99,
        "customer": "test_user",
    }, signature=None, headers={"X-Webhook-Id": "wh-001"})

    assert result["ok"], f"Webhook接收失败: {result.get('message')}"
    assert result["event_id"] is not None
    assert len(result.get("dispatch_targets", [])) > 0

    # 验证落湖
    lake_data = sink_svc.query_lake_data("order_created")
    assert lake_data["total"] > 0, "数据未成功写入数据湖"


# ──────────────────────────────────────────────────────────
# 链路2: API批量拉取 → 数据转换 → 质量校验 → 落湖
# ──────────────────────────────────────────────────────────
def test_chain_2_batch_to_lake(isolated_store):
    """验证批量API拉取到落湖的完整链路。"""
    ep_svc = ApiEndpointService()
    batch_svc = BatchIngestService()
    quality_svc = DataQualityRuleService()

    # 创建端点
    ep_svc.create_entry({
        "端点编号": "EP-001",
        "端点名称": "订单API",
        "请求方法": "GET",
        "基础URL": "https://api.example.com/orders",
        "认证类型": "Bearer",
        "超时秒数": "10",
        "重试次数": "3",
        "端点版本": "v1",
    })
    ep_id = app.persistence.json_store.get_all("api_endpoint")[0]["id"]

    # 创建质量规则
    quality_svc.create_entry({
        "规则编号": "QR-001",
        "数据来源": "",
        "校验字段": "name",
        "校验表达式": "not_empty",
        "阈值下界": "0",
        "阈值上界": "0",
        "阻断策略": "警告",
        "规则版本": "v1",
    })
    qrs = app.persistence.json_store.get_all("data_quality_rule")
    for qr in qrs:
        quality_svc.run_action(int(qr["id"]), "标记生效")

    # 创建批量任务并启动
    batch_svc.create_entry({
        "任务编号": "BI-001",
        "关联端点": str(ep_id),
        "Cron表达式": "0 */1 * * *",
        "分页策略": "分页拉取",
        "并行批次": "3",
        "断点续传": "是",
        "失败动作": "记录并继续",
        "任务版本": "v1",
    })
    bi_id = app.persistence.json_store.get_all("batch_ingest")[0]["id"]
    batch_svc.run_action(bi_id, "启动任务")

    # 执行批量拉取
    result = batch_svc.execute_batch(bi_id, max_pages=3, page_size=10)

    assert result["ok"], f"批量拉取失败: {result.get('message')}"
    assert result["records_fetched"] > 0
    assert result["records_sunk"] > 0

    # 检查落湖
    sink_svc = DataLakeSinkService()
    lake_data = sink_svc.query_lake_data("batch_ingest")
    assert lake_data["total"] > 0, "批量数据未写入数据湖"


# ──────────────────────────────────────────────────────────
# 链路3: 死信队列 → 分析 → 重放 → 正常流程
# ──────────────────────────────────────────────────────────
def test_chain_3_dlq_to_replay(isolated_store, monkeypatch):
    """验证死信队列从入队到重放的完整链路。"""
    mq_svc = MqConsumerService()
    dlq_svc = DlqHandlerService()

    # 固定随机数，确定性触发消费失败（<0.2 即失败），避免概率用例偶发零死信
    import random as _random
    monkeypatch.setattr(_random, "random", lambda: 0.1)

    # 创建消费者
    mq_svc.create_entry({
        "配置编号": "MQ-001",
        "队列名称": "order_queue",
        "消费组名": "order_group",
        "偏移策略": "earliest",
        "批量大小": "5",
        "超时时间": "5000",
        "死信队列": "order_dlq",
        "消费状态": "运行中",
    })
    mq_id = app.persistence.json_store.get_all("mq_consumer")[0]["id"]
    mq_svc.run_action(mq_id, "启动消费")

    # 消费一批消息（会触发部分死信）
    result = mq_svc.consume_batch(mq_id, max_messages=10)

    # 检查死信统计
    dlq_stats = dlq_svc.get_dlq_stats()
    assert dlq_stats["total_dlq"] > 0, "应该有消息进入死信队列"

    # 分析失败原因
    analysis = dlq_svc.analyze_failure_reasons()
    assert analysis["total"] > 0
    assert len(analysis["categories"]) > 0

    # 批量重放死信
    dlq_ids = [r["id"] for r in app.persistence.json_store.get_all("dlq_handler") if r["status"] in ("草稿", "待处置")]
    if dlq_ids:
        replay_result = dlq_svc.batch_replay(dlq_ids)
        assert replay_result["ok"]
        assert replay_result["replayed"] + replay_result["failed"] == len(dlq_ids)


# ──────────────────────────────────────────────────────────
# 链路4: 批量回补 → 增量水位 → 落湖 → 监控
# ──────────────────────────────────────────────────────────
def test_chain_4_backfill_to_monitor(isolated_store):
    """验证回补任务到监控的完整链路。"""
    backfill_svc = BackfillJobService()
    cursor_svc = IncreCursorService()
    monitor_svc = IntegrationMonitorService()

    # 创建回补任务
    backfill_svc.create_entry({
        "任务编号": "BF-001",
        "关联端点": "1",
        "起始日期": "2024-01-01",
        "截止日期": "2024-01-05",
        "并行度": "3",
        "覆盖策略": "追加不覆写",
        "完成水位": "0/5",
        "失败区间": "无",
    })
    bf_id = app.persistence.json_store.get_all("backfill_job")[0]["id"]
    backfill_svc.run_action(bf_id, "启动回补")

    # 执行回补
    result = backfill_svc.execute_backfill(bf_id)
    assert result["ok"]
    assert result["days_processed"] > 0
    assert result["records_backfilled"] > 0

    # 检查增量水位更新
    pos = cursor_svc.get_position(1)
    assert pos["cursor_count"] >= 0

    # 检查监控指标
    metrics = monitor_svc.collect_global_metrics()
    assert "health_score" in metrics
    assert metrics["health_score"] >= 0


# ──────────────────────────────────────────────────────────
# 链路5: 事件接收 → 去重过滤 → 分发 → 归档
# ──────────────────────────────────────────────────────────
def test_chain_5_dedup_to_archive(isolated_store):
    """验证去重过滤链路的正确性。"""
    dedup_svc = DedupFilterService()
    webhook_svc = WebhookChannelService()
    archive_svc = EventArchiveService()

    # 创建去重规则
    dedup_svc.create_entry({
        "规则编号": "DF-001",
        "事件类型": "通用",
        "去重键列": "id",
        "比较方式": "精确匹配",
        "时间窗口": "24小时",
        "相似度阈值": "0.95",
        "冲突策略": "跳过",
        "规则版本": "v1",
    })
    df_id = app.persistence.json_store.get_all("dedup_filter")[0]["id"]
    dedup_svc.run_action(df_id, "标记生效")

    # 第一次去重检查
    r1 = dedup_svc.check_duplicate("test_event", {"id": "EVT-001", "data": "hello"})
    assert not r1["is_duplicate"], "首次事件应该是非重复"

    # 第二次同样的事件
    r2 = dedup_svc.check_duplicate("test_event", {"id": "EVT-001", "data": "hello"})
    assert r2["is_duplicate"], "第二次相同事件应该被去重"

    # 获取去重统计
    stats = dedup_svc.get_dedup_stats()
    assert stats["total_duplicates"] >= 1

    # 创建归档策略
    archive_svc.create_entry({
        "策略编号": "AR-001",
        "事件类型": "test_event",
        "归档条件": "days>0",
        "归档路径": "/archive/test",
        "压缩格式": "zlib",
        "保留天数": "90",
        "清理方式": "自动删除",
        "恢复超时": "60",
    })
    ar_id = app.persistence.json_store.get_all("event_archive")[0]["id"]
    archive_svc.run_action(ar_id, "标记生效")

    # 执行归档
    arch_result = archive_svc.execute_archive(ar_id)
    assert arch_result["ok"]


# ──────────────────────────────────────────────────────────
# 链路6: 调用追踪 → 监控统计 → 质量评分
# ──────────────────────────────────────────────────────────
def test_chain_6_trace_to_monitor(isolated_store):
    """验证调用追踪到监控统计的链路。"""
    trace_svc = CallTraceLogService()
    monitor_svc = IntegrationMonitorService()

    # 记录多条调用
    for i in range(10):
        lat = 50 + i * 100  # 递增延迟
        trace_svc._record_trace(
            endpoint_id=1,
            action=f"测试调用{i}",
            latency_ms=lat,
            status="正常",
            record_count=1,
        )

    # 记录一次慢调用
    trace_svc._record_trace(
        endpoint_id=1,
        action="慢调用测试",
        latency_ms=5000,
        status="正常",
        record_count=1,
    )

    # 性能统计
    perf = trace_svc.get_performance_stats(endpoint_id=1, hours=24)
    assert perf["total"] > 0
    assert perf["p50_ms"] > 0
    assert perf.get("slow_calls", 0) >= 1 or perf["by_status"].get("慢调用", 0) >= 1

    # 监控聚合
    metrics = monitor_svc.collect_global_metrics()
    assert "trace" in metrics
    assert "health_score" in metrics

    # 健康状态
    health = monitor_svc.get_health_status()
    assert "health_score" in health


# ──────────────────────────────────────────────────────────
# 综合集成测试
# ──────────────────────────────────────────────────────────
def test_full_integration_pipeline(isolated_store):
    """完整集成测试：端到端数据链路。"""
    # 1. 注册API端点
    ep_svc = ApiEndpointService()
    ep_svc.create_entry({
        "端点编号": "EP-FULL",
        "端点名称": "全链路测试端点",
        "请求方法": "GET",
        "基础URL": "https://api.example.com/data",
        "认证类型": "Bearer",
        "超时秒数": "30",
        "重试次数": "3",
        "端点版本": "v1",
    })
    ep_id = app.persistence.json_store.get_all("api_endpoint")[0]["id"]

    # 2. 生成认证凭证
    auth_svc = ApiAuthStoreService()
    cred_result = auth_svc.generate_credential(ep_id, "APIKey", validity_days=90)
    assert cred_result["ok"]

    # 3. 创建Webhook通道
    wh_svc = WebhookChannelService()
    wh_svc.create_entry({
        "通道编号": "WH-FULL",
        "通道名称": "全链路测试通道",
        "接收路径": "/webhook/full",
        "签名密钥": "key123",
        "验证方式": "NONE",
        "并发上限": "50",
        "回调地址": "https://callback.test.com",
        "通道版本": "v1",
    })
    wh_id = app.persistence.json_store.get_all("webhook_channel")[0]["id"]
    wh_svc.run_action(wh_id, "启用通道")

    # 4. 发送Webhook事件
    event_result = wh_svc.receive_webhook(wh_id, {
        "type": "integration_test",
        "id": str(uuid.uuid4()),
        "value": 42,
        "status": "active",
    })
    assert event_result["ok"]

    # 5. 验证所有数据快照
    services = [
        (ApiEndpointService(), "api_endpoint"),
        (WebhookChannelService(), "webhook_channel"),
        (EventSchemaService(), "event_schema"),
        (MqConsumerService(), "mq_consumer"),
        (DlqHandlerService(), "dlq_handler"),
        (DataLakeSinkService(), "data_lake_sink"),
        (DataTransformService(), "data_transform"),
        (DataQualityRuleService(), "data_quality_rule"),
        (DispatchRuleService(), "dispatch_rule"),
        (DedupFilterService(), "dedup_filter"),
        (BatchIngestService(), "batch_ingest"),
        (IncreCursorService(), "incre_cursor"),
        (BackfillJobService(), "backfill_job"),
        (CallTraceLogService(), "call_trace_log"),
        (RateLimitConfigService(), "rate_limit_config"),
        (ApiAuthStoreService(), "api_auth_store"),
        (EventArchiveService(), "event_archive"),
        (IntegrationMonitorService(), "integration_monitor"),
    ]

    for svc, name in services:
        snapshot = svc.domain_snapshot()
        assert snapshot["module"] == name, f"{name} domain_snapshot module mismatch"
        assert "count" in snapshot
        assert "by_status" in snapshot

    # 6. 全局监控
    monitor = IntegrationMonitorService()
    metrics = monitor.collect_global_metrics()
    assert metrics["health_score"] >= 0