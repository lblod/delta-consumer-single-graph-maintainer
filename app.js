import { CronJob } from 'cron';
import { app, errorHandler } from 'mu';
import {
    CRON_PATTERN_DELTA_SYNC, INITIAL_SYNC_JOB_OPERATION, SERVICE_NAME
} from './config';
import { waitForDatabase } from './lib/database';
import { ProcessingQueue } from './lib/processing-queue';
import { cleanupJob, getJobs } from './lib/job';
import { startDeltaSync } from './pipelines/delta-sync';
import { startInitialSync } from './pipelines/initial-sync';

const deltaSyncQueue = new ProcessingQueue('delta-sync-queue');

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

waitForDatabase(startInitialSync);

new CronJob(CRON_PATTERN_DELTA_SYNC, async function() {
  const now = new Date().toISOString();
  console.info(`Delta sync triggered by cron job at ${now}`);
  deltaSyncQueue.addJob(startDeltaSync);
}, null, true);

/*
 * ENDPOINTS CURRENTLY MEANT FOR DEBUGGING
 */

app.post('/initial-sync-jobs', async function( _, res ){
  startInitialSync();
  res.send({ msg: 'Started initial sync job' });
});

app.delete('/initial-sync-jobs', async function( _, res ){
  const jobs = await getJobs(INITIAL_SYNC_JOB_OPERATION);
  for(const { job } of jobs){
    await cleanupJob(job);
  }
  res.send({ msg: 'Initial sync jobs cleaned' });
});

app.post('/delta-sync-jobs', async function( _, res ){
  startDeltaSync();
  res.send({ msg: 'Started delta sync job' });
});

app.use(errorHandler);
