import { app, errorHandler } from 'mu';
import { CronJob } from 'cron';
import {
  SERVICE_NAME,
  INITIAL_SYNC_JOB_OPERATION,
  CRON_PATTERN_DELTA_SYNC
} from './config';
import { waitForDatabase } from './lib/database';
import { cleanupJobs, getJobs } from './lib/utils';
import { startInitialSync } from './lib/initial-sync/initial-sync';
import { startDeltaSync } from './lib/delta-sync/delta-sync';

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

waitForDatabase(startInitialSync);

new CronJob(CRON_PATTERN_DELTA_SYNC, async function() {
  const now = new Date().toISOString();
  console.info(`Delta sync triggered by cron job at ${now}`);
  await startDeltaSync();
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
  await cleanupJobs(jobs);
  res.send({ msg: 'Initial sync jobs cleaned' });
});

app.get('/delta-sync-jobs', async function( _, res ){
  startDeltaSync();
  res.send({ msg: 'Started delta sync job' });
});

app.use(errorHandler);

