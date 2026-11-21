"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import dlq, pipeline, resource

# 固定路径路由（dlq、pipeline）必须排在 resource 的 /api/{module} 动态路径之前，
# 否则 /api/pipeline/... 与 /api/dlq/... 会被动态路径抢先匹配。
ROUTERS = [dlq, pipeline, resource]
