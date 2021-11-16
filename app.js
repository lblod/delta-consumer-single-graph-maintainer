import { app, errorHandler } from 'mu';
import { CronJob } from 'cron';
import {
  SERVICE_NAME,
  INITIAL_SYNC_JOB_OPERATION,
  CRON_PATTERN_DELTA_SYNC,
  CRON_PATTERN_FILE_SYNC
} from './config';
import { waitForDatabase } from './lib/database';
import { cleanupJobs, getJobs } from './lib/utils';
import { startInitialSync } from './lib/initial-sync/initial-sync';
import { startDeltaSync } from './lib/delta-sync/delta-sync';
import { startFileSync }  from './lib/delta-sync/file-sync';

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

waitForDatabase(startInitialSync);

new CronJob(CRON_PATTERN_DELTA_SYNC, async function() {
  const now = new Date().toISOString();
  console.info(`Delta sync triggered by cron job at ${now}`);
  console.log("Delta sync not executed, uncomment the code for that in the CronJob");
  //await startDeltaSync();
}, null, true);

new CronJob(CRON_PATTERN_FILE_SYNC, async function() {
  const now = new Date().toISOString();
  console.info(`File sync triggered by cron job at ${now}`);
  console.log("File sync not executed, uncomment the code for that in the CronJob");
  //await startFileSync();
}, null, true);

/*
 * ENDPOINTS CURRENTLY MEANT FOR DEBUGGING
 */

app.get('/initial-sync-jobs', async function( _, res ){
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

app.get("/file-sync", async function (req, res) {
  startFileSync();
  res.json({ msg: "Started file sync" });
});

app.use(errorHandler);

