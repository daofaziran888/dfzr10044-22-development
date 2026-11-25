<template>
  <section class="page" data-module="recovery-console">
    <header class="page-head">
      <div>
        <h2>恢复证据控制台</h2>
        <p class="page-desc">
          按库域和湖表标识受理恢复证据清单，领取后沿冻结的快照与分区文件逐份读取；
          中断可续读，旧版文件回收支持等待释放或读前确认副本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-overview" @click="reload">刷新概览</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">湖表条目</span>
        <strong class="stat-value">{{ overview?.table_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">恢复清单</span>
        <strong class="stat-value">{{ overview?.manifest_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">恢复作业</span>
        <strong class="stat-value">{{ overview?.job_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">作业状态分布</span>
        <strong class="stat-value">{{ statusSummary }}</strong>
      </article>
    </div>

    <h3>湖表恢复概览（含改名后条目）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>库域</th>
          <th>湖表名称</th>
          <th>曾用名</th>
          <th>湖表标识</th>
          <th>文件数</th>
          <th>可用快照</th>
          <th>清单数（待受理）</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in overview?.entries ?? []" :key="entry.table_uid">
          <td>{{ entry.database_domain }}</td>
          <td>{{ entry.table_name }}<span v-if="entry.renamed" class="tag">已改名</span></td>
          <td>{{ entry.previous_names.join('、') || '—' }}</td>
          <td>{{ entry.table_uid }}</td>
          <td>{{ entry.file_count }}</td>
          <td>{{ entry.available_snapshots }}</td>
          <td>{{ entry.manifest_count }}（{{ entry.open_manifests }}）</td>
        </tr>
      </tbody>
    </table>

    <h3>受理与领取恢复清单</h3>
    <form class="filter-bar" @submit.prevent="createAndClaim">
      <label class="filter-item">
        <span>库域</span>
        <input v-model="form.domain" data-testid="form-domain" placeholder="如 lake_ods" required />
      </label>
      <label class="filter-item">
        <span>湖表名</span>
        <input v-model="form.tableName" data-testid="form-table" placeholder="如 sales" required />
      </label>
      <button class="btn primary" type="button" data-testid="create-manifest" @click="createManifest">
        受理清单
      </button>
      <button class="btn primary" type="submit" data-testid="claim-manifest">受理并领取</button>
    </form>

    <h3>恢复清单</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>清单编号</th>
          <th>库域</th>
          <th>湖表名</th>
          <th>快照标识</th>
          <th>分区</th>
          <th>文件数/行数</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="manifest in manifests" :key="manifest.manifest_id">
          <td>{{ manifest.manifest_id }}</td>
          <td>{{ manifest.database_domain }}</td>
          <td>{{ manifest.table_name }}</td>
          <td>{{ manifest.snapshot_id }}</td>
          <td>{{ manifest.partition_keys.join('、') }}</td>
          <td>{{ manifest.file_count }} / {{ manifest.total_rows }}</td>
          <td>{{ manifest.status }}</td>
          <td class="row-actions">
            <button
              v-if="manifest.status === 'open'"
              class="link"
              type="button"
              :data-testid="`claim-${manifest.manifest_id}`"
              @click="claim(manifest.manifest_id)"
            >
              领取
            </button>
            <span v-else>领取人：{{ manifest.claimed_by }}</span>
          </td>
        </tr>
      </tbody>
    </table>

    <h3>恢复作业</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>作业编号</th>
          <th>库域/湖表</th>
          <th>快照</th>
          <th>已读/总数</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="job in jobs" :key="job.job_id" :data-status="job.status">
          <td>{{ job.job_id }}</td>
          <td>{{ job.database_domain }} / {{ job.file_table_key }}</td>
          <td>{{ job.snapshot_id }}</td>
          <td>{{ job.read_count }} / {{ job.file_ids.length }}</td>
          <td>{{ job.status }}<span v-if="job.failure_reason" class="tag">{{ job.failure_reason }}</span></td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`read-${job.job_id}`" @click="readNext(job.job_id)">
              读取下一份
            </button>
            <button class="link" type="button" @click="pause(job.job_id)">暂停</button>
            <button class="link" type="button" @click="resume(job.job_id)">恢复执行</button>
            <button class="link" type="button" @click="complete(job.job_id)">完成</button>
            <button class="link" type="button" @click="showUnread(job.job_id)">可补取部分</button>
          </td>
        </tr>
      </tbody>
    </table>

    <h3>旧版文件回收与值班查询</h3>
    <form class="filter-bar" @submit.prevent="submitReclamation">
      <label class="filter-item">
        <span>物理表键</span>
        <input v-model="reclaim.lakeTable" data-testid="reclaim-table" placeholder="如 lake_ods.sales" required />
      </label>
      <label class="filter-item">
        <span>文件编号</span>
        <input v-model="reclaim.fileId" data-testid="reclaim-file" placeholder="分区文件 file_id" required />
      </label>
      <button class="btn primary" type="submit" data-testid="submit-reclaim">提交回收</button>
    </form>
    <table class="data-table">
      <thead>
        <tr>
          <th>回收单</th>
          <th>文件</th>
          <th>状态</th>
          <th>持有方</th>
          <th>副本</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="order in reclamations" :key="order.order_id">
          <td>{{ order.order_id }}</td>
          <td>{{ order.file_id }}</td>
          <td>{{ order.status }}</td>
          <td>{{ order.blocker_holder ?? '—' }}</td>
          <td>{{ order.replica_copy_id ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="confirmCopy(order.order_id)">确认副本</button>
            <button class="link" type="button" @click="execute(order.order_id)">执行回收</button>
            <button class="link" type="button" @click="queryHolder(order.lake_table, order.file_id)">
              查持有者
            </button>
          </td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span v-if="message" class="error-text" data-testid="console-message">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { recoveryApi } from '@/api/backend-service'

type Overview = Awaited<ReturnType<typeof recoveryApi.overview>>
type ManifestRow = {
  manifest_id: string
  database_domain: string
  table_name: string
  snapshot_id: string
  partition_keys: string[]
  file_count: number
  total_rows: number
  status: string
  claimed_by: string | null
}
type JobRow = {
  job_id: string
  database_domain: string
  file_table_key: string
  snapshot_id: string
  file_ids: string[]
  read_count: number
  status: string
  failure_reason: string
}
type ReclamationRow = {
  order_id: string
  lake_table: string
  file_id: string
  status: string
  blocker_holder: string | null
  replica_copy_id: string | null
}

const overview = ref<Overview | null>(null)
const manifests = ref<ManifestRow[]>([])
const jobs = ref<JobRow[]>([])
const reclamations = ref<ReclamationRow[]>([])
const message = ref('')
const form = ref({ domain: 'lake_ods', tableName: 'sales' })
const reclaim = ref({ lakeTable: '', fileId: '' })

const statusSummary = computed(() => {
  const byStatus = overview.value?.jobs_by_status ?? {}
  return Object.entries(byStatus).map(([status, count]) => `${status}:${count}`).join(' / ') || '—'
})

function notify(error: unknown, fallback: string) {
  message.value = error instanceof Error ? error.message : fallback
}

async function reload() {
  try {
    const [ov, mani, jobList, recl] = await Promise.all([
      recoveryApi.overview(),
      recoveryApi.listManifests() as Promise<{ items: ManifestRow[] }>,
      recoveryApi.listJobs() as Promise<{ items: JobRow[] }>,
      recoveryApi.listReclamations() as Promise<{ items: ReclamationRow[] }>,
    ])
    overview.value = ov
    manifests.value = mani.items
    jobs.value = jobList.items
    reclamations.value = recl.items
  } catch (error) {
    notify(error, '恢复概览读取失败')
  }
}

async function createManifest() {
  try {
    await recoveryApi.createManifest(form.value.domain, form.value.tableName)
    message.value = '恢复证据清单已受理'
    await reload()
  } catch (error) {
    notify(error, '清单受理失败')
  }
}

async function claim(manifestId: string) {
  try {
    await recoveryApi.claimManifest(manifestId)
    message.value = '清单已领取，恢复作业已建立'
    await reload()
  } catch (error) {
    notify(error, '领取失败')
  }
}

async function createAndClaim() {
  try {
    const created = await recoveryApi.createManifest(form.value.domain, form.value.tableName)
    const manifestId = (created as { entry: ManifestRow }).entry.manifest_id
    await recoveryApi.claimManifest(manifestId)
    message.value = '清单受理并领取成功'
    await reload()
  } catch (error) {
    notify(error, '受理并领取失败')
  }
}

async function readNext(jobId: string) {
  try {
    const result = await recoveryApi.readNext(jobId) as {
      file_id: string | null
      already_read?: boolean
      success?: boolean
      failure_reason?: string
    }
    message.value = result.already_read
      ? '清单全部文件已读取，无新增输入'
      : `已读取文件：${result.file_id ?? '—'}`
    await reload()
  } catch (error) {
    notify(error, '读取失败')
  }
}

async function pause(jobId: string) {
  try {
    await recoveryApi.pauseJob(jobId)
    message.value = '作业已暂停，租约继续持有'
    await reload()
  } catch (error) {
    notify(error, '暂停失败')
  }
}

async function resume(jobId: string) {
  try {
    await recoveryApi.resumeJob(jobId)
    message.value = '作业已恢复，从断点续读'
    await reload()
  } catch (error) {
    notify(error, '恢复执行失败')
  }
}

async function complete(jobId: string) {
  try {
    const summary = await recoveryApi.completeJob(jobId) as { files_read: string[] }
    message.value = `恢复完成，共读取 ${summary.files_read.length} 个文件，引用已释放`
    await reload()
  } catch (error) {
    notify(error, '完成失败')
  }
}

async function showUnread(jobId: string) {
  try {
    const result = await recoveryApi.unreadFiles(jobId) as { unread_file_ids: string[] }
    message.value = `可补取文件 ${result.unread_file_ids.length} 个：${result.unread_file_ids.join('、') || '无'}`
  } catch (error) {
    notify(error, '可补取部分查询失败')
  }
}

async function submitReclamation() {
  try {
    await recoveryApi.submitReclamation(reclaim.value.lakeTable, reclaim.value.fileId)
    message.value = '旧版文件回收单已提交'
    await reload()
  } catch (error) {
    notify(error, '回收提交失败')
  }
}

async function confirmCopy(orderId: string) {
  try {
    await recoveryApi.confirmReplicaCopy(orderId)
    message.value = '已确认可继续使用的文件副本'
    await reload()
  } catch (error) {
    notify(error, '副本确认失败')
  }
}

async function execute(orderId: string) {
  try {
    await recoveryApi.executeReclamation(orderId)
    message.value = '旧版文件已回收'
    await reload()
  } catch (error) {
    notify(error, '回收执行失败')
  }
}

async function queryHolder(lakeTable: string, fileId: string) {
  try {
    const holder = await recoveryApi.fileHolder(lakeTable, fileId) as {
      holder: string | null
      holder_kind?: string
    }
    message.value = holder.holder
      ? `文件 ${fileId} 当前由 ${holder.holder_kind ?? ''}:${holder.holder} 持有`
      : `文件 ${fileId} 当前无持有者`
  } catch (error) {
    notify(error, '持有者查询失败')
  }
}

onMounted(reload)
</script>
