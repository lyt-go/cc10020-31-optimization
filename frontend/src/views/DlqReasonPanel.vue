<template>
  <section class="dlq-panel" data-testid="dlq-reason-panel">
    <h3 class="dlq-title">死信原因筛选与积压复核</h3>

    <form class="dlq-filter" @submit.prevent="reloadFirstPage">
      <label>
        <span>失败原因关键词</span>
        <input v-model="form.失败原因" data-testid="dlq-filter-reason" placeholder="原失败或重放失败原因" />
      </label>
      <label>
        <span>原因分类</span>
        <select v-model="form.原因分类" data-testid="dlq-filter-category">
          <option value="">全部分类</option>
          <option v-for="cat in categories" :key="cat" :value="cat">{{ cat }}</option>
        </select>
      </label>
      <label>
        <span>失败阶段</span>
        <select v-model="form.失败阶段" data-testid="dlq-filter-phase">
          <option value="">全部阶段</option>
          <option value="原失败">原失败</option>
          <option value="重放失败">重放失败</option>
        </select>
      </label>
      <label>
        <span>原因口径</span>
        <select v-model="form.原因口径" data-testid="dlq-filter-usable">
          <option value="">全部口径</option>
          <option value="处置">可继续处置</option>
          <option value="回查">只供回查</option>
        </select>
      </label>
      <label>
        <span>原始队列</span>
        <input v-model="form.原始队列" data-testid="dlq-filter-queue" placeholder="源队列名称" />
      </label>
      <div class="dlq-filter-actions">
        <button class="btn primary" data-testid="dlq-query" type="submit">检索</button>
        <button class="btn ghost" type="button" @click="resetFilters">重置</button>
      </div>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="dlq-error">{{ errorMessage }}</p>

    <div class="dlq-stat-row">
      <article class="dlq-stat">
        <span class="stat-label">检索命中消息</span>
        <strong data-testid="dlq-list-total">{{ pageTotal }}</strong>
      </article>
      <article class="dlq-stat">
        <span class="stat-label">积压消息总数（按原始消息）</span>
        <strong data-testid="dlq-backlog-total">{{ backlog.积压消息总数 }}</strong>
      </article>
      <article class="dlq-stat">
        <span class="stat-label">待处置原因分组</span>
        <strong>{{ backlog.原因分组.length }}</strong>
      </article>
    </div>

    <h4 class="dlq-subtitle">原因分组（处置口径 / 回查口径）</h4>
    <table class="data-table dlq-table" data-testid="dlq-reason-table">
      <thead>
        <tr>
          <th>失败阶段</th>
          <th>失败原因</th>
          <th>原因分类</th>
          <th>消息数</th>
          <th>可处置</th>
          <th>只回查</th>
          <th>口径</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="option in reasonOptions" :key="`${option.失败阶段}-${option.失败原因}`">
          <td>{{ option.失败阶段 }}</td>
          <td>{{ option.失败原因 }}</td>
          <td>{{ option.原因分类 }}</td>
          <td>{{ option.消息数 }}</td>
          <td>{{ option.可处置消息数 }}</td>
          <td>{{ option.回查消息数 }}</td>
          <td>
            <span class="dlq-badge" :class="badgeClass(option.原因口径)">{{ option.原因口径 }}</span>
          </td>
        </tr>
        <tr v-if="!reasonOptions.length">
          <td colspan="7" class="empty-state">当前筛选条件下暂无原因分组</td>
        </tr>
      </tbody>
    </table>

    <h4 class="dlq-subtitle">积压台账（与检索清单同一份筛选规则）</h4>
    <div class="dlq-queues" data-testid="dlq-queues">
      <span v-for="(count, queue) in backlog.按原始队列" :key="queue" class="dlq-queue-tag">
        {{ queue }}：{{ count }}
      </span>
      <span v-if="!Object.keys(backlog.按原始队列).length" class="empty-state">无积压</span>
    </div>

    <h4 class="dlq-subtitle">死信检索清单（同一消息一轮分页只计一次）</h4>
    <table class="data-table dlq-table" data-testid="dlq-records-table">
      <thead>
        <tr>
          <th>记录编号</th>
          <th>原始队列</th>
          <th>原失败原因</th>
          <th>重放失败原因</th>
          <th>原因分类</th>
          <th>口径</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in records" :key="String(row.id)" data-testid="dlq-record-row">
          <td>{{ row.记录编号 }}</td>
          <td>{{ row.原始队列 }}</td>
          <td>{{ row.原失败原因 ?? '—' }}</td>
          <td>{{ row.重放失败原因 ?? '—' }}</td>
          <td>{{ (row.原因分类 ?? []).join('、') || '—' }}</td>
          <td>
            <span class="dlq-badge" :class="badgeClass(row.原因口径)">{{ row.原因口径 }}</span>
          </td>
          <td>{{ row.status }}</td>
        </tr>
        <tr v-if="!records.length">
          <td colspan="7" class="empty-state">暂无命中死信</td>
        </tr>
      </tbody>
    </table>
    <div class="dlq-pager">
      <button class="btn" type="button" :disabled="!canPrev" @click="prevPage">上一页</button>
      <span>第 {{ pageIndex + 1 }} 页</span>
      <button class="btn" type="button" :disabled="!nextCursor" @click="nextPage">下一页</button>
    </div>

    <h4 class="dlq-subtitle">值班复核台账</h4>
    <div class="dlq-review-form">
      <input v-model="reviewTeam" data-testid="dlq-review-team" placeholder="班组名称，如：一班" />
      <input v-model="reviewRemark" placeholder="复核备注（可选）" />
      <button class="btn primary" data-testid="dlq-review-create" type="button" @click="createReviewFromFilter">
        按当前筛选发起积压复核
      </button>
    </div>
    <table class="data-table dlq-table" data-testid="dlq-review-table">
      <thead>
        <tr>
          <th>台账编号</th>
          <th>班组</th>
          <th>口径</th>
          <th>消息数</th>
          <th>筛选口径</th>
          <th>复核状态</th>
          <th>签收人</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="review in reviews" :key="review.id">
          <td>
            <button class="link" type="button" @click="openReview(review)">{{ review.台账编号 }}</button>
          </td>
          <td>{{ review.班组名称 }}</td>
          <td><span class="dlq-badge" :class="badgeClass(review.原因口径)">{{ review.原因口径 }}</span></td>
          <td>{{ review.积压消息总数 }}</td>
          <td class="dlq-scope">{{ scopeText(review.筛选口径) }}</td>
          <td>{{ review.复核状态 }}</td>
          <td>{{ review.签收人 || '—' }}</td>
          <td>
            <button
              v-if="review.复核状态 !== '已签收'"
              class="link"
              type="button"
              :data-testid="`dlq-review-sign-${review.id}`"
              @click="sign(review)"
            >
              签收
            </button>
            <span v-else class="empty-state">已签收</span>
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="activeReview" class="modal-backdrop" data-testid="dlq-review-modal" @click.self="activeReview = null">
      <div class="modal dlq-review-modal">
        <h3>复核详情 {{ activeReview.台账编号 }}</h3>
        <p><strong>班组：</strong>{{ activeReview.班组名称 }}　<strong>口径：</strong>{{ activeReview.原因口径 }}</p>
        <p><strong>当时筛选口径：</strong>{{ scopeText(activeReview.筛选口径) }}</p>
        <table class="data-table dlq-table">
          <thead>
            <tr><th>记录编号</th><th>原始队列</th><th>原失败原因</th><th>重放失败原因</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="snap in activeReview.消息快照" :key="String(snap.dlq_id)">
              <td>{{ snap.记录编号 }}</td>
              <td>{{ snap.原始队列 }}</td>
              <td>{{ snap.原失败原因 ?? '—' }}</td>
              <td>{{ snap.重放失败原因 ?? '—' }}</td>
              <td>{{ snap.处置状态 }}</td>
            </tr>
          </tbody>
        </table>
        <div class="dlq-filter-actions">
          <button class="btn" type="button" @click="activeReview = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  createReview,
  fetchBacklog,
  fetchDlqPage,
  fetchReasonOptions,
  listReviews,
  signReview,
} from '@/api/dlq-service'
import type { BacklogSummary, ReasonOption, ReviewRecord } from '@/api/dlq-service'

const PAGE_SIZE = 10
const categories = ['超时类', '格式类', '数据类', '连接类', '认证类', '其他']

const form = reactive<Record<string, string>>({
  失败原因: '',
  原因分类: '',
  失败阶段: '',
  原因口径: '',
  原始队列: '',
})

const reasonOptions = ref<ReasonOption[]>([])
const backlog = ref<BacklogSummary>({
  筛选口径: {},
  积压消息总数: 0,
  积压记录数: 0,
  按原始队列: {},
  原因分组: [],
})
const records = ref<Record<string, any>[]>([])
const reviews = ref<ReviewRecord[]>([])
const pageTotal = ref(0)
const nextCursor = ref<string | null>(null)
const cursorStack = ref<string[]>([])
const pageIndex = ref(0)
const errorMessage = ref('')
const reviewTeam = ref('')
const reviewRemark = ref('')
const activeReview = ref<ReviewRecord | null>(null)

const canPrev = () => cursorStack.value.length > 1

function activeFilters(): Record<string, string> {
  return Object.fromEntries(Object.entries(form).filter(([, value]) => value.trim()))
}

function badgeClass(scope: string): string {
  if (scope === '处置') return 'dlq-badge-action'
  if (scope === '回查') return 'dlq-badge-reference'
  return 'dlq-badge-mixed'
}

function scopeText(scope: Record<string, string>): string {
  const labels: Record<string, string> = {
    keyword: '原因关键词',
    category: '原因分类',
    phase: '失败阶段',
    usable: '原因口径',
    queue: '原始队列',
    status: '处置状态',
  }
  const parts = Object.entries(scope)
    .filter(([, value]) => value)
    .map(([key, value]) => `${labels[key] ?? key}=${value}`)
  return parts.length ? parts.join('；') : '全量口径'
}

async function loadReasonData() {
  const filters = activeFilters()
  const [reasons, backlogData] = await Promise.all([
    fetchReasonOptions(filters),
    fetchBacklog(filters),
  ])
  reasonOptions.value = reasons.options
  backlog.value = backlogData
}

async function loadPage(cursor?: string) {
  const result = await fetchDlqPage(activeFilters(), PAGE_SIZE, cursor)
  records.value = result.items
  pageTotal.value = result.total
  nextCursor.value = result.next_cursor
}

async function reloadFirstPage() {
  errorMessage.value = ''
  cursorStack.value = []
  pageIndex.value = 0
  try {
    await Promise.all([loadReasonData(), loadPage(undefined)])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '死信数据加载失败'
  }
}

function resetFilters() {
  Object.keys(form).forEach((key) => {
    form[key] = ''
  })
  void reloadFirstPage()
}

async function nextPage() {
  if (!nextCursor.value) return
  cursorStack.value.push(nextCursor.value)
  pageIndex.value += 1
  try {
    await loadPage(nextCursor.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '翻页失败'
  }
}

async function prevPage() {
  if (cursorStack.value.length <= 1) {
    cursorStack.value = []
    pageIndex.value = 0
    await loadPage(undefined)
    return
  }
  cursorStack.value.pop()
  const target = cursorStack.value[cursorStack.value.length - 1]
  pageIndex.value = Math.max(0, pageIndex.value - 1)
  await loadPage(target)
}

async function loadReviews() {
  try {
    const result = await listReviews()
    reviews.value = result.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核台账加载失败'
  }
}

async function createReviewFromFilter() {
  errorMessage.value = ''
  if (!reviewTeam.value.trim()) {
    errorMessage.value = '请填写班组名称'
    return
  }
  try {
    await createReview(reviewTeam.value.trim(), activeFilters(), reviewRemark.value.trim())
    reviewRemark.value = ''
    await loadReviews()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发起复核失败'
  }
}

async function sign(review: ReviewRecord) {
  const signer = window.prompt(`签收复核台账 ${review.台账编号}，请输入签收人`)
  if (!signer) return
  try {
    await signReview(review.id, signer)
    await loadReviews()
    const refreshed = reviews.value.find((item) => item.id === review.id)
    if (refreshed && activeReview.value?.id === review.id) {
      activeReview.value = refreshed
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '签收失败'
  }
}

function openReview(review: ReviewRecord) {
  activeReview.value = review
}

onMounted(() => {
  void reloadFirstPage()
  void loadReviews()
})
</script>

<style scoped>
.dlq-panel {
  border: 1px solid #d9e0ec;
  border-radius: 10px;
  padding: 16px;
  margin: 16px 0;
  background: #fbfcfe;
}

.dlq-title {
  margin: 0 0 12px;
  font-size: 16px;
}

.dlq-subtitle {
  margin: 18px 0 8px;
  font-size: 14px;
  color: #33415c;
}

.dlq-filter {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px;
}

.dlq-filter label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 12px;
  color: #5a6a85;
}

.dlq-filter input,
.dlq-filter select,
.dlq-review-form input {
  padding: 6px 8px;
  border: 1px solid #c6d0e0;
  border-radius: 6px;
  font-size: 13px;
}

.dlq-filter-actions {
  display: flex;
  gap: 8px;
  align-items: flex-end;
}

.dlq-stat-row {
  display: flex;
  gap: 12px;
  margin: 14px 0;
  flex-wrap: wrap;
}

.dlq-stat {
  flex: 1 1 160px;
  background: #fff;
  border: 1px solid #e3e8f2;
  border-radius: 8px;
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.dlq-table {
  font-size: 13px;
}

.dlq-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
}

.dlq-badge-action {
  background: #e6f4ff;
  color: #155e9c;
}

.dlq-badge-reference {
  background: #f1f2f5;
  color: #667085;
}

.dlq-badge-mixed {
  background: #fff4e0;
  color: #9a6400;
}

.dlq-queues {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.dlq-queue-tag {
  background: #eef2ff;
  border-radius: 6px;
  padding: 4px 10px;
  font-size: 13px;
}

.dlq-pager {
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 12px 0;
}

.dlq-review-form {
  display: flex;
  gap: 8px;
  margin-bottom: 10px;
  flex-wrap: wrap;
}

.dlq-review-form input {
  flex: 1 1 180px;
}

.dlq-scope {
  max-width: 260px;
  font-size: 12px;
  color: #5a6a85;
}

.dlq-review-modal {
  max-width: 880px;
  width: 92vw;
  max-height: 84vh;
  overflow: auto;
}
</style>
