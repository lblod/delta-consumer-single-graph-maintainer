import { app, errorHandler } from 'mu';
import fetch from 'node-fetch';
import { DISABLE_DELTA_INGEST, INGEST_INTERVAL, SERVICE_NAME } from './config';
import { waitForDatabase } from './lib/database';
import { getNextSyncTask, getRunningSyncTask, scheduleSyncTask, setTaskFailedStatus } from './lib/sync-task';
import { storeError } from './lib/utils';

waitForDatabase().then(async () => {
  try {
    const runningTask = await getRunningSyncTask();
    if (runningTask) {
      console.log(`Task <${runningTask.uri.value}> is still ongoing at startup. Updating its status to failed.`);
      await setTaskFailedStatus(runningTask.uri.value);
    }
    if (INGEST_INTERVAL > 0) {
      await automatedIngestionScheduling();
    }
  } catch (error) {
    await storeError(`Unexpected error while booting the service: ${error}`);
  }
});

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine ^^`);
});

async function automatedIngestionScheduling() {
  try {
    console.log(`Scheduled ingestion at ${new Date().toISOString()}`);
    fetch('http://localhost/schedule-ingestion/', {method: 'POST'});
    setTimeout(automatedIngestionScheduling, INGEST_INTERVAL);
  } catch (error) {
    await storeError(`Unexpected error while schedueling automated ingestion: ${error}`);
  }
}

app.post('/schedule-ingestion', async function(req, res) {
  try {
    if (DISABLE_DELTA_INGEST) {
      console.log('Delta ingestion is disabled. Skipping.');
      return res.status(200).end();
    } else {
      await scheduleSyncTask();

      const isRunning = await getRunningSyncTask();
      if (!isRunning) {
        const task = await getNextSyncTask();
        if (task) {
          console.log(`Start ingesting new delta files since ${task.since.toISOString()}`);
          try {
            await task.execute();
            return res.status(202).end();
          } catch(error) {
            console.log(`Closing sync task with failure state.`);
            await setTaskFailedStatus(task.uri);
            throw error;
          }
        } else {
          console.log(`No scheduled sync task found. Did the insertion of a new task just fail?`);
          return res.status(200).end();
        }
      } else {
        console.log('A sync task is already running. A new task is scheduled and will start when the previous task finishes.');
        return res.status(201).end();
      }
    }
  } catch (error) {
    await storeError(`Unexpected error while ingesting: ${error}`);
    return res.status(500).end();
  }
});

app.use(errorHandler);
