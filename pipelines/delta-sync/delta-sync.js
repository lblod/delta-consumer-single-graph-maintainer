import {
  DISABLE_DELTA_INGEST,
  STATUS_SUCCESS,
  WAIT_FOR_INITIAL_SYNC,
  INITIAL_SYNC_JOB_OPERATION,
  JOBS_GRAPH, SERVICE_NAME
} from '../../config';
import {
  getNextSyncTask,
  getRunningSyncTask,
  scheduleSyncTask,
  setTaskFailedStatus
} from './sync-task';
import { getLatestJobForOperation } from '../../lib/job';
import { createError } from '../../lib/error';
export async function startDeltaSync() {
  try {
    console.info(`DISABLE_DELTA_INGEST: ${DISABLE_DELTA_INGEST}`);
    if (!DISABLE_DELTA_INGEST) {
      const previousInitialSyncJob = await getLatestJobForOperation(INITIAL_SYNC_JOB_OPERATION);
      if (WAIT_FOR_INITIAL_SYNC && !(previousInitialSyncJob && previousInitialSyncJob.status == STATUS_SUCCESS)) {
        console.log('No successful initial sync job found. Not scheduling delta ingestion.');
      } else {
        console.log('Initial sync was success, proceeding in Normal operation mode: ingest deltas');
        const runningTask = await getRunningSyncTask();
        if (runningTask) {
          console.log(`Task <${runningTask.uri.value}> is still ongoing at startup. Updating its status to failed.`);
          await setTaskFailedStatus(runningTask.uri.value);
        }
        await runDeltaSync();
      }
    }
    else {
      console.warn('Automated delta ingest disabled');
    }
  }
  catch(e) {
    console.log(e);
    await createError(JOBS_GRAPH, SERVICE_NAME, `Unexpected error while running normal sync task: ${e}`);
  }
}

async function runDeltaSync() {
  try {
    await scheduleSyncTask();

    const isRunning = await getRunningSyncTask();
    if (!isRunning) {
      const task = await getNextSyncTask();
      if (task) {
        console.log(`Start ingesting new delta files since ${task.since.toISOString()}`);
        try {
          await task.execute();
        } catch(error) {
          console.log(`Closing sync task with failure state.`);
          await setTaskFailedStatus(task.uri);
          throw error;
        }
      } else {
        console.log(`No scheduled sync task found. Did the insertion of a new task just fail?`);
      }
    } else {
      console.log('A sync task is already running. A new task is scheduled and will start when the previous task finishes.');
    }
  } catch (error) {
    await createError(JOBS_GRAPH, SERVICE_NAME, `Unexpected error while ingesting: ${error}`);
  }
}
