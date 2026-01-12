import type { SyncJobConfig, SyncProgress } from './types';
import { apiRequest } from './http';

/**
 * Starts a new sync job
 * 
 * Real implementation would call:
 * POST /api/sync-jobs
 * Body: SyncJobConfig
 */
export async function startSyncJob(config: SyncJobConfig): Promise<SyncProgress> {
  return apiRequest<SyncProgress>('/api/sync-jobs', {
    method: 'POST',
    body: JSON.stringify(config),
  });
}

/**
 * Gets the current progress of a sync job
 * 
 * Real implementation would call:
 * GET /api/sync-jobs/:jobId
 */
export async function getSyncProgress(jobId: string): Promise<SyncProgress | null> {
  try {
    return await apiRequest<SyncProgress>(`/api/sync-jobs/${encodeURIComponent(jobId)}`);
  } catch (error) {
    return null;
  }
}

/**
 * Cancels a running sync job
 * 
 * Real implementation would call:
 * DELETE /api/sync-jobs/:jobId
 */
export async function cancelSyncJob(jobId: string): Promise<boolean> {
  try {
    await apiRequest<void>(`/api/sync-jobs/${encodeURIComponent(jobId)}`, {
      method: 'DELETE',
    });
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Gets all sync jobs (for history)
 */
export async function getAllJobs(): Promise<SyncProgress[]> {
  return apiRequest<SyncProgress[]>('/api/sync-jobs');
}

/**
 * Clears completed/failed jobs from memory
 */
export async function clearCompletedJobs(): Promise<void> {
  await apiRequest<void>('/api/sync-jobs/completed', {
    method: 'DELETE',
  });
}
