import { request } from './client'
import type { PageResult } from '@/data/types'

export type ReasonOption = {
  失败阶段: string
  失败原因: string
  原因分类: string
  消息数: number
  可处置消息数: number
  回查消息数: number
  原因口径: string
  dlq_ids: number[]
}

export type BacklogSummary = {
  筛选口径: Record<string, string>
  积压消息总数: number
  积压记录数: number
  按原始队列: Record<string, number>
  原因分组: ReasonOption[]
}

export type ReviewRecord = {
  id: number
  台账编号: string
  班组名称: string
  原因口径: string
  筛选口径: Record<string, string>
  原因分组: ReasonOption[]
  消息快照: Array<{
    dlq_id: number
    记录编号: string
    原始队列: string
    原失败原因: string | null
    重放失败原因: string | null
    失败原因明细: Array<{ phase: string; reason: string; category: string }>
    处置状态: string
    消息身份: string
  }>
  积压消息总数: number
  复核状态: string
  签收人: string
  签收时间: string
  备注: string
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error('死信原因数据读取失败')
  return (await response.json()) as T
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '请求失败')
  }
  return (await response.json()) as T
}

export function fetchReasonOptions(filters: Record<string, string>): Promise<{
  options: ReasonOption[]
  backlog_total: number
  usable_scopes: string[]
}> {
  const query = new URLSearchParams(filters).toString()
  return getJson(`/api/dlq/reasons${query ? `?${query}` : ''}`)
}

export function fetchBacklog(filters: Record<string, string>): Promise<BacklogSummary> {
  const query = new URLSearchParams(filters).toString()
  return getJson(`/api/dlq/backlog${query ? `?${query}` : ''}`)
}

export function fetchDlqPage(
  filters: Record<string, string>,
  size: number,
  cursor?: string,
): Promise<PageResult & { next_cursor: string | null }> {
  const params = new URLSearchParams(filters)
  params.set('size', String(size))
  if (cursor) params.set('cursor', cursor)
  return getJson(`/api/dlq_handler?${params.toString()}`)
}

export function createReview(team: string, filters: Record<string, string>, remark = '') {
  return postJson<ReviewRecord>('/api/dlq/reviews', { team, filters, remark })
}

export function listReviews(): Promise<{ items: ReviewRecord[]; total: number }> {
  return getJson('/api/dlq/reviews')
}

export function signReview(reviewId: number, signer: string) {
  return postJson<{ ok: boolean; message: string }>(
    `/api/dlq/reviews/${reviewId}/sign`,
    { signer },
  )
}
