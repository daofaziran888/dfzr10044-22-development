import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 恢复证据专用接口：清单受理、作业领取、断点续读与旧版文件回收 ----

async function recoveryPost<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '恢复证据操作失败')
  }
  return await response.json()
}

async function recoveryGet<T>(path: string, params: Record<string, string> = {}): Promise<T> {
  const query = new URLSearchParams(params).toString()
  const response = await request(query ? `${path}?${query}` : path)
  if (!response.ok) throw new Error('恢复证据读取失败')
  return await response.json()
}

export const recoveryApi = {
  overview: () => recoveryGet<{
    table_count: number
    manifest_count: number
    job_count: number
    jobs_by_status: Record<string, number>
    entries: Array<{
      database_domain: string
      table_uid: string
      table_name: string
      previous_names: string[]
      renamed: boolean
      available_snapshots: number
      file_count: number
      manifest_count: number
      open_manifests: number
    }>
  }>('/api/recovery/overview'),
  listManifests: (databaseDomain?: string) =>
    recoveryGet('/api/recovery/manifests', databaseDomain ? { database_domain: databaseDomain } : {}),
  createManifest: (databaseDomain: string, tableName: string, snapshotId?: string) =>
    recoveryPost('/api/recovery/manifests', {
      database_domain: databaseDomain,
      table_name: tableName,
      snapshot_id: snapshotId ?? null,
    }),
  claimManifest: (manifestId: string) =>
    recoveryPost(`/api/recovery/manifests/${manifestId}/claim`, {}),
  listJobs: (databaseDomain?: string) =>
    recoveryGet('/api/recovery/jobs', databaseDomain ? { database_domain: databaseDomain } : {}),
  listReclamations: (lakeTable?: string) =>
    recoveryGet('/api/recovery/reclamations', lakeTable ? { lake_table: lakeTable } : {}),
  readNext: (jobId: string) => recoveryPost(`/api/recovery/jobs/${jobId}/read-next`),
  pauseJob: (jobId: string) => recoveryPost(`/api/recovery/jobs/${jobId}/pause`),
  resumeJob: (jobId: string) => recoveryPost(`/api/recovery/jobs/${jobId}/resume`),
  completeJob: (jobId: string) => recoveryPost(`/api/recovery/jobs/${jobId}/complete`),
  unreadFiles: (jobId: string) => recoveryGet(`/api/recovery/jobs/${jobId}/unread`),
  submitReclamation: (lakeTable: string, fileId: string) =>
    recoveryPost('/api/recovery/reclamations', { lake_table: lakeTable, file_id: fileId }),
  confirmReplicaCopy: (orderId: string) =>
    recoveryPost('/api/recovery/reclamations/confirm-copy', { order_id: orderId }),
  executeReclamation: (orderId: string) =>
    recoveryPost('/api/recovery/reclamations/execute', { order_id: orderId }),
  fileHolder: (lakeTable: string, fileId: string) =>
    recoveryGet('/api/recovery/files/holder', { lake_table: lakeTable, file_id: fileId }),
  renameTable: (databaseDomain: string, oldName: string, newName: string) =>
    recoveryPost('/api/recovery/tables/rename', {
      database_domain: databaseDomain,
      old_name: oldName,
      new_name: newName,
    }),
}
