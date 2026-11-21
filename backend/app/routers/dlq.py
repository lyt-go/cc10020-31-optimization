"""死信原因筛选与值班复核专用接口。

与通用资源路由共享同一套取数规则（见 :mod:`app.services.dlq_reasons`）：
- /reasons 提供原因分组筛选数据（集成监控跨模块复用同一份结果）；
- /backlog 提供积压台账，总量按原始消息去重；
- /reviews 维护值班复核台账，签收后仍可回查当时筛选口径与两类失败原因。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.services.dlq_handler import DlqHandlerService
from app.services.dlq_reasons import USABLE_ACTION, USABLE_REFERENCE

router = APIRouter(prefix="/api/dlq", tags=["死信原因筛选"])

# 原因筛选允许透传的查询参数
_FILTER_KEYS = ("失败原因", "原因分类", "失败阶段", "原因口径", "原始队列", "处置状态")


def _filters_from_request(request: Request) -> dict[str, str]:
    filters: dict[str, str] = {}
    for key in _FILTER_KEYS:
        value = request.query_params.get(key)
        if value and value.strip():
            filters[key] = value.strip()
    return _normalize_filters(filters)


def _normalize_filters(raw: dict[str, str] | None) -> dict[str, str]:
    """把对外查询参数（含「处置状态」）归一为 build_reason_filter 识别的键。"""
    filters = dict(raw or {})
    status = filters.pop("处置状态", None)
    if status:
        filters.setdefault("status", status)
    return filters


class ReviewCreatePayload(BaseModel):
    team: str
    filters: dict[str, str] | None = None
    remark: str = ""


class ReviewSignPayload(BaseModel):
    signer: str


@router.get("/reasons")
def list_reason_options(request: Request) -> dict:
    """死信原因筛选数据：按失败阶段+原因+分类分组，标注处置/回查口径。"""
    svc = DlqHandlerService()
    filters = _filters_from_request(request)
    options = svc.get_reason_options(filters)
    backlog = svc.get_backlog_summary(filters)
    return {
        "options": options,
        "backlog_total": backlog["积压消息总数"],
        "usable_scopes": [USABLE_ACTION, USABLE_REFERENCE],
    }


@router.get("/backlog")
def get_backlog(request: Request) -> dict:
    """积压台账：与检索清单同一份筛选规则，按原始消息统计积压总量。"""
    svc = DlqHandlerService()
    return svc.get_backlog_summary(_filters_from_request(request))


@router.post("/reviews")
def create_review(payload: ReviewCreatePayload) -> dict:
    """班组从原因筛选结果发起积压复核，分组快照写入值班复核台账。"""
    team = payload.team.strip()
    if not team:
        raise HTTPException(status_code=400, detail="班组名称不能为空")
    svc = DlqHandlerService()
    return svc.create_review(team=team, filters=_normalize_filters(payload.filters), remark=payload.remark)


@router.get("/reviews")
def list_reviews(status: str | None = Query(default=None)) -> dict:
    """值班复核台账列表。"""
    svc = DlqHandlerService()
    rows = svc.list_reviews(status=status)
    return {"items": rows, "total": len(rows)}


@router.get("/reviews/{review_id}")
def get_review(review_id: int) -> dict:
    """复核详情：已签收记录仍可重新定位筛选口径、原失败及重放失败。"""
    svc = DlqHandlerService()
    record = svc.get_review(review_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"复核台账 {review_id} 不存在")
    return record


@router.post("/reviews/{review_id}/sign")
def sign_review(review_id: int, payload: ReviewSignPayload) -> dict:
    """签收复核台账。"""
    signer = payload.signer.strip()
    if not signer:
        raise HTTPException(status_code=400, detail="签收人不能为空")
    svc = DlqHandlerService()
    record, message = svc.sign_review(review_id, signer)
    if record is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "entry": record}
