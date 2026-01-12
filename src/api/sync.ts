import type { SyncJobConfig, SyncProgress, SyncError } from './types';
import { delay } from './mockData';

// Active sync jobs
const activeJobs = new Map<string, SyncProgress>();

/**
 * Starts a new sync job
 * 
 * Real implementation would call:
 * POST /api/sync-jobs
 * Body: SyncJobConfig
 */
export async function startSyncJob(config: SyncJobConfig): Promise<SyncProgress> {
  await delay(500);

  const jobId = `sync_${Date.now()}_${Math.random().toString(36).substring(7)}`;
  
  const progress: SyncProgress = {
    jobId,
    status: 'pending',
    totalRecords: 0,
    processedRecords: 0,
    insertedRecords: 0,
    updatedRecords: 0,
    skippedRecords: 0,
    errors: [],
    startedAt: new Date().toISOString(),
  };

  activeJobs.set(jobId, progress);

  // Start the mock sync process
  simulateSyncProgress(jobId, config);

  return progress;
}

/**
 * Gets the current progress of a sync job
 * 
 * Real implementation would call:
 * GET /api/sync-jobs/:jobId
 */
export async function getSyncProgress(jobId: string): Promise<SyncProgress | null> {
  await delay(100);
  return activeJobs.get(jobId) || null;
}

/**
 * Cancels a running sync job
 * 
 * Real implementation would call:
 * DELETE /api/sync-jobs/:jobId
 */
export async function cancelSyncJob(jobId: string): Promise<boolean> {
  await delay(200);
  
  const job = activeJobs.get(jobId);
  if (job && job.status === 'running') {
    job.status = 'failed';
    job.errors.push({
      recordId: '',
      message: 'Job cancelled by user',
    });
    return true;
  }
  return false;
}

/**
 * Simulates sync progress updates
 */
function simulateSyncProgress(jobId: string, config: SyncJobConfig): void {
  const job = activeJobs.get(jobId);
  if (!job) return;

  // Simulate fetching record count
  setTimeout(() => {
    job.status = 'running';
    job.totalRecords = Math.floor(Math.random() * 500) + 100; // 100-600 records
  }, 500);

  // Simulate progress updates
  const batchSize = 25;
  let processed = 0;
  
  const progressInterval = setInterval(() => {
    const job = activeJobs.get(jobId);
    if (!job || job.status !== 'running') {
      clearInterval(progressInterval);
      return;
    }

    // Process a batch
    const remaining = job.totalRecords - processed;
    const toProcess = Math.min(batchSize, remaining);
    processed += toProcess;
    
    job.processedRecords = processed;
    
    // Distribute between insert/update/skip based on sync mode
    if (config.syncMode === 'upsert') {
      const inserted = Math.floor(toProcess * 0.6);
      const updated = Math.floor(toProcess * 0.35);
      const skipped = toProcess - inserted - updated;
      
      job.insertedRecords += inserted;
      job.updatedRecords += updated;
      job.skippedRecords += skipped;
    } else {
      // Insert-only mode
      const inserted = Math.floor(toProcess * 0.85);
      const skipped = toProcess - inserted;
      
      job.insertedRecords += inserted;
      job.skippedRecords += skipped;
    }

    // Simulate occasional errors (2% chance per batch)
    if (Math.random() < 0.02) {
      const error: SyncError = {
        recordId: `REC_${Math.random().toString(36).substring(7)}`,
        field: 'monthly_income',
        message: 'Value exceeds maximum allowed length',
      };
      job.errors.push(error);
    }

    // Check if complete
    if (processed >= job.totalRecords) {
      clearInterval(progressInterval);
      job.status = 'completed';
      job.completedAt = new Date().toISOString();
    }
  }, 300); // Update every 300ms for realistic feel
}

/**
 * Gets all sync jobs (for history)
 */
export function getAllJobs(): SyncProgress[] {
  return Array.from(activeJobs.values());
}

/**
 * Clears completed/failed jobs from memory
 */
export function clearCompletedJobs(): void {
  for (const [jobId, job] of activeJobs) {
    if (job.status === 'completed' || job.status === 'failed') {
      activeJobs.delete(jobId);
    }
  }
}
