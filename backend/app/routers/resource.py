"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def collect_filters(service, request: Request) -> dict[str, str]:
    """从查询参数收集筛选字段：通用列表字段 + 领域服务声明的扩展筛选字段。"""
    allowed = set(service.LIST_FIELDS) | set(getattr(service, "REASON_FILTER_FIELDS", []))
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in allowed and value.strip()
    }


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> PageResult[dict]:
    service = require_service(module)
    filters = collect_filters(service, request)
    try:
        result = service.list_entries(
            filters=filters, keyword=keyword, status=status,
            page=page, size=size, cursor=cursor)
    except ValueError as exc:
        # 分页游标与筛选条件不一致等客户端可纠正问题
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    # 带游标分页的领域服务返回三元组：(items, total, next_cursor)
    if isinstance(result, tuple) and len(result) == 3:
        items, total, next_cursor = result
        return PageResult(items=items, total=total, page=page, size=size, next_cursor=next_cursor)
    items, total = result
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = collect_filters(service, request)
    result = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    items = result[0]
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
