import {
  DISABLE_INITIAL_SYNC,
  STATUS_FAILED,
  STATUS_SUCCESS,
  TASK_SUCCESS_STATUS
} from '../../config';
import { createSyncTask } from '../delta-sync/sync-task';
import { scheduleInitialSyncTask } from './initial-sync-task';
import { getLatestInitialSyncJob, scheduleInitialSyncJob } from './initial-sync-job';
import { getLatestDumpFile } from './dump-file';
import { storeError } from '../utils';

export async function startInitialSync() {
  try {
    console.info(`DISABLE_INITIAL_SYNC: ${DISABLE_INITIAL_SYNC}`);
    if(!DISABLE_INITIAL_SYNC) {
      const initialSyncJob = await getLatestInitialSyncJob();
      // In following case we can safely (re)schedule an initial sync
      if (!initialSyncJob || initialSyncJob.status == STATUS_FAILED) {
        console.log(`No initial sync has run yet, or previous failed (see: ${initialSyncJob ? initialSyncJob.uri : 'N/A'})`);
        console.log(`(Re)starting initial sync`);

        const job = await runInitialSync();

        console.log(`${job.uri} has status ${job.status}, start ingesting deltas`);
      } else if (initialSyncJob.status !== STATUS_SUCCESS){
        throw `Unexpected status for ${initialSyncJob.uri}: ${initialSyncJob.status}. Check in the database what went wrong`;
      } else {
        console.log(`Initial sync <${initialSyncJob.uri}> has already run.`);
      }
    } else {
      console.warn('Initial sync disabled');
    }
  }
  catch(e) {
    console.log(e);
    await storeError(`Unexpected error while booting the service: ${e}`);
  }
}

async function runInitialSync() {
  let job;
  let task;

  try {
    // Note: they get status busy
    job = await scheduleInitialSyncJob();
    task = await scheduleInitialSyncTask(job);

    const dumpFile = await getLatestDumpFile();
    task.dumpFile = dumpFile;
    await task.execute();

    //Some glue to coordinate the nex sync-task. It needs to know from where it needs to start syncing
    await createSyncTask(task.dumpFile.issued, TASK_SUCCESS_STATUS);

    await job.updateStatus(STATUS_SUCCESS);

    return job;
  }
  catch(e) {
    console.log(`Something went wrong while doing the initial sync. Closing task with failure state.`);
    console.trace(e);
    if(task)
      await task.closeWithFailure(e);
    if(job)
      await job.closeWithFailure();
    throw e;
  }
}
