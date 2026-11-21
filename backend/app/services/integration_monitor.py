"""集成监控看板领域服务：全局指标聚合、健康度评分与实时告警。

从各领域服务收集指标数据，聚合计算全局健康度评分、
成功率趋势和异常告警信息。
跨模块关联：call_trace_log（调用指标）、data_quality_rule（质量指标）、data_lake_sink（落湖指标）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class IntegrationMonitorService(BaseModuleService):
    MODULE = "integration_monitor"
    ENTITY = "监控指标"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["指标编号", "监控对象", "成功率", "平均延迟", "数据吞吐", "失败分类", "最近告警", "监控状态"]
    REQUIRED_FIELDS = ["指标编号", "监控对象", "成功率"]
    STATUS_ORDER = ["草稿", "待校验", "监控中", "已静默"]
    ACTION_RULES = {"提交校验": "待校验", "启动监控": "监控中", "静默告警": "已静默"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._alert_history: list[dict[str, Any]] = []
        self._health_scores: dict[str, float] = {}

    def collect_global_metrics(self) -> dict[str, Any]:
        """从所有领域服务收集全局监控指标。

        聚合维度：
        1. 调用链路：成功率、P95延迟、慢调用数
        2. 数据质量：校验通过率、阻断次数
        3. 数据落湖：写入量、存储大小
        4. 增量水位：同步进度
        5. 死信队列：积压量、重放成功率
        6. 回补任务：完成率

        Returns:
            包含各维度指标和综合健康度评分的字典。
        """
        now = datetime.now(timezone.utc)
        metrics = {
            "collected_at": now.isoformat(),
            "modules": {},
        }

        # 调用链路指标 (call_trace_log)
        try:
            from app.services.call_trace_log import CallTraceLogService
            trace_svc = CallTraceLogService()
            perf = trace_svc.get_performance_stats(hours=24)
            metrics["trace"] = {
                "total_calls": perf.get("total", 0),
                "success_rate": perf.get("success_rate", 0),
                "avg_latency_ms": perf.get("avg_latency_ms", 0),
                "p95_ms": perf.get("p95_ms", 0),
            }
        except Exception as e:
            metrics["trace"] = {"error": str(e)}

        # 数据质量指标 (data_quality_rule)
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            report = quality_svc.get_quality_report()
            metrics["quality"] = {
                "active_rules": report.get("active_rules", 0),
                "recent_checks": report.get("total_checks", 0),
                "recent_failures": report.get("recent_failures", 0),
            }
        except Exception as e:
            metrics["quality"] = {"error": str(e)}

        # 数据落湖指标 (data_lake_sink)
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            sink_svc = DataLakeSinkService()
            lake_stats = sink_svc.get_lake_stats()
            metrics["lake"] = {
                "total_files": lake_stats.get("total_files", 0),
                "total_size_mb": lake_stats.get("total_size_mb", 0),
            }
        except Exception as e:
            metrics["lake"] = {"error": str(e)}

        # 死信队列指标 (dlq_handler)：跨模块直接复用死信原因筛选数据结果
        try:
            from app.services.dlq_handler import DlqHandlerService
            dlq_svc = DlqHandlerService()
            dlq_stats = dlq_svc.get_dlq_stats()
            # 与死信模块同一份筛选规则下的原因分组与积压口径
            reason_options = dlq_svc.get_reason_options()
            backlog = dlq_svc.get_backlog_summary()
            metrics["dlq"] = {
                "total_dlq": dlq_stats.get("total_dlq", 0),
                "pending": dlq_stats.get("pending_dispatch", 0),
                "replayed": dlq_stats.get("replayed_count", 0),
                "backlog_total": backlog.get("积压消息总数", 0),
                "reason_options": reason_options,
                "backlog_by_queue": backlog.get("按原始队列", {}),
            }
        except Exception as e:
            metrics["dlq"] = {"error": str(e)}

        # 增量水位指标 (incre_cursor)
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            positions = cursor_svc.get_all_positions()
            metrics["cursors"] = {
                "active_cursors": len(positions),
                "total_position": sum(positions.values()) if positions else 0,
            }
        except Exception as e:
            metrics["cursors"] = {"error": str(e)}

        # 批量接入指标 (batch_ingest)
        try:
            from app.services.batch_ingest import BatchIngestService
            batch_svc = BatchIngestService()
            rows = json_store.get_all("batch_ingest")
            metrics["batch"] = {
                "total_tasks": len(rows),
                "running_tasks": sum(1 for r in rows if r.get("status") == "运行中"),
            }
        except Exception as e:
            metrics["batch"] = {"error": str(e)}

        # 计算综合健康度
        health = self._calculate_health_score(metrics)
        metrics["health_score"] = health["score"]
        metrics["health_status"] = health["status"]
        metrics["health_issues"] = health["issues"]

        # 触发告警检查
        alerts = self._check_alerts(metrics)
        metrics["alerts"] = alerts

        return metrics

    def _calculate_health_score(self, metrics: dict[str, Any]) -> dict[str, Any]:
        """计算综合健康度评分（0-100）。"""
        score = 100.0
        issues = []

        # 调用成功率扣分
        trace_data = metrics.get("trace", {})
        if isinstance(trace_data, dict) and "success_rate" in trace_data:
            success_rate = trace_data["success_rate"]
            if success_rate < 95:
                deduction = (95 - success_rate) * 2
                score -= deduction
                issues.append(f"调用成功率 {success_rate}% 低于95% (扣{deduction:.0f}分)")

        # 死信积压扣分
        dlq_data = metrics.get("dlq", {})
        if isinstance(dlq_data, dict) and "pending" in dlq_data:
            pending = dlq_data["pending"]
            if pending > 10:
                score -= min(pending, 20)
                issues.append(f"死信积压 {pending} 条")

        # 质量失败扣分
        quality_data = metrics.get("quality", {})
        if isinstance(quality_data, dict) and "recent_failures" in quality_data:
            failures = quality_data["recent_failures"]
            if failures > 5:
                score -= min(failures * 2, 15)
                issues.append(f"质量校验失败 {failures} 次")

        score = max(0.0, min(100.0, score))

        status = "健康"
        if score < 60:
            status = "严重"
        elif score < 80:
            status = "警告"

        return {"score": round(score, 2), "status": status, "issues": issues}

    def _check_alerts(self, metrics: dict[str, Any]) -> list[dict[str, Any]]:
        """检查是否触发告警条件。"""
        alerts = []
        now = datetime.now(timezone.utc)

        # 成功率告警
        trace_data = metrics.get("trace", {})
        if isinstance(trace_data, dict) and trace_data.get("success_rate", 100) < 90:
            alert = {
                "level": "critical",
                "type": "low_success_rate",
                "message": f"调用成功率降至 {trace_data['success_rate']}%",
                "timestamp": now.isoformat(),
            }
            alerts.append(alert)
            self._alert_history.append(alert)

        # 死信积压告警
        dlq_data = metrics.get("dlq", {})
        if isinstance(dlq_data, dict) and dlq_data.get("pending", 0) > 20:
            alert = {
                "level": "warning",
                "type": "dead_letter_backlog",
                "message": f"死信积压 {dlq_data['pending']} 条",
                "timestamp": now.isoformat(),
            }
            alerts.append(alert)
            self._alert_history.append(alert)

        # P95延迟告警
        if isinstance(trace_data, dict) and trace_data.get("p95_ms", 0) > 5000:
            alert = {
                "level": "warning",
                "type": "high_latency",
                "message": f"P95延迟 {trace_data['p95_ms']}ms 超过5000ms阈值",
                "timestamp": now.isoformat(),
            }
            alerts.append(alert)
            self._alert_history.append(alert)

        return alerts

    def get_health_status(self) -> dict[str, Any]:
        """获取当前健康状态。"""
        metrics = self.collect_global_metrics()
        return {
            "health_score": metrics.get("health_score", 0),
            "health_status": metrics.get("health_status", "未知"),
            "issues": metrics.get("health_issues", []),
            "alerts_count": len(metrics.get("alerts", [])),
            "collected_at": metrics.get("collected_at"),
        }

    def get_dlq_reason_dashboard(self, filters: dict[str, str] | None = None) -> dict[str, Any]:
        """跨模块消费死信原因筛选数据：监控侧直接读取死信模块的分组与积压口径。"""
        from app.services.dlq_handler import DlqHandlerService
        dlq_svc = DlqHandlerService()
        return {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "filter_scope": filters or {},
            "reason_options": dlq_svc.get_reason_options(filters),
            "backlog": dlq_svc.get_backlog_summary(filters),
        }

    def export_dlq_review_ledger(self, team: str, filters: dict[str, str] | None = None,
                                 remark: str = "") -> dict[str, Any]:
        """把监控侧使用的死信原因分组结果写入值班复核台账。"""
        from app.services.dlq_handler import DlqHandlerService
        dlq_svc = DlqHandlerService()
        return dlq_svc.create_review(team=team, filters=filters, remark=remark or "集成监控跨模块发起")

    def get_alerts(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取告警历史。"""
        return sorted(self._alert_history, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]

    def domain_snapshot(self) -> dict[str, Any]:
        """输出集成监控看板的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        health = self.get_health_status()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "集成监控看板数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "health_score": health.get("health_score", 0),
            "health_status": health.get("health_status", "未知"),
            "alerts_total": len(self._alert_history),
            "operation": self.OPERATION_TERM,
        }