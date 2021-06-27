import mu, { sparqlEscapeDateTime, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import fetch from 'node-fetch';
import {
  START_FROM_DELTA_TIMESTAMP,
  SYNC_FILES_ENDPOINT,
  JOBS_GRAPH,
  JOB_CREATOR_URI,
  TASK_NOT_STARTED_STATUS,
  TASK_ONGOING_STATUS,
  TASK_SUCCESS_STATUS,
  TASK_FAILED_STATUS
} from '../../config';
import DeltaFile from './delta-file';

class SyncTask {
  constructor({uri, since, created, status}) {
    /** Uri of the sync task */
    this.uri = uri;

    /**
     * Datetime as Data object since when delta files should be retrieved from the producer service
     */
    this.since = since;

    /**
     * Datetime as Data object when the task was created in the triplestore
     */
    this.created = created;

    /**
     * Current status of the sync task as stored in the triplestore
     */
    this.status = status;

    /**
     * Time in ms of the latest successfully ingested delta file.
     * Will be updated while delta files are being consumed.
     */
    this.latestDeltaMs = Date.parse(since.toISOString());

    /**
     * List of delta files to be ingested for this task
     * I.e. delta files generated since timestamp {since}
     * retrieved from the producer server just before
     * the start of the task execution
     *
     * @type Array of DeltaFile
     */
    this.files = [];

    /**
     * Number of already successful ingested delta files for this task
     */
    this.handledFiles = 0;

    /**
     * Progress status of the handling of delta files.
     * This status is only used during execution and never persisted in the store.
     * Possible values: [ 'notStarted', 'progressing', 'failed' ]
     */
    this.progressStatus = 'notStarted';
  }

  /**
   * Get datetime as Date object of the latest successfully ingested delta file
   */
  get latestDelta() {
    return new Date(this.latestDeltaMs);
  }

  /**
   * Get the total number of files to be ingested for this task
   */
  get totalFiles() {
    return this.files.length;
  }

  /**
   * Execute the sync task
   * I.e. consume the delta files one-by-one as long as there are delta files
   * or until ingestion of a file fails
   *
   * @public
   */
  async execute() {
    try {
      await this.persistStatus(TASK_ONGOING_STATUS);
      this.files = await getUnconsumedFiles(this.since);
      console.log(`Found ${this.totalFiles} new files to be consumed`);
      if (this.totalFiles) {
        await this.consumeNext();
      } else {
        console.log(`No files to consume. Finished sync task successfully.`);
        console.log(`Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`);
        await this.persistLatestDelta(this.latestDeltaMs);
        await this.persistStatus(TASK_SUCCESS_STATUS);
      }
    } catch (e) {
      console.log(`Something went wrong while consuming the files`);
      console.log(e);
      await this.persistLatestDelta(this.latestDeltaMs);
      throw e;
    }
  }

  /**
   * Recursive function to consume the next delta file in the files array
   *
   * @private
   */
  async consumeNext() {
    const file = this.files[this.handledFiles];
    await file.consume(async (file, isSuccess) => {
      this.handledFiles++;
      console.log(`Consumed ${this.handledFiles}/${this.totalFiles} files`);
      await this.updateProgressStatus(file, isSuccess);

      if (this.progressStatus === 'progressing' && this.handledFiles < this.totalFiles) {
        await this.consumeNext();
      } else {
        if (this.progressStatus === 'failed') {
          await this.persistStatus(TASK_FAILED_STATUS);
          console.log(
            `Failed to finish sync task. Skipping the remaining files.
            Most recent delta file successfully consumed is created at ${this.latestDelta.toISOString()}.`
          );
        } else {
          await this.persistStatus(TASK_SUCCESS_STATUS);
          console.log(
            `Finished sync task successfully. Ingested ${this.totalFiles} files.
            Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`
          );
        }
      }
    });
  }

  /**
   * Update the progress status of the delta handling and write the latest ingested delta timestamp to the store.
   * I.e. update the progress status to 'progressing' and update the latest delta timestamp on success.
   * Update the progress status to 'failed' on failure
   *
   * @param file {DeltaFile} Ingested delta file
   * @param isSuccess {boolean} Flag to indicate success of ingestion of the given delta file
   * @private
   */
  async updateProgressStatus(file, isSuccess) {
    if (isSuccess && this.progressStatus !== 'failed') {
      this.progressStatus = 'progressing';

      const deltaMs = Date.parse(file.created);
      if (deltaMs > this.latestDeltaMs) {
        await this.persistLatestDelta(deltaMs);
      }
    } else if (!isSuccess) {
      this.progressStatus = 'failed';
    }
  }

  /**
   * Perists the given timestamp as timestamp of the latest consumed delta file in the triple store.
   *
   * At any moment the latest ext:deltaUntil timestamp on a task, either in failed/ongoing/success state,
   * should reflect the timestamp of the latest delta file that has been completly and successfully consumed.
   * Therefore, the ext:deltaUntil needs to be updated immediately after every delta file consumption.
   *
   * @param deltaMs {int} Timestamp in milliseconds of the latest successfully consumed delta file
   * @private
   */
  async persistLatestDelta(deltaMs) {
    this.latestDeltaMs = deltaMs;

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> ext:deltaUntil ?latestDelta .
        }
      }
    `);

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      INSERT {
        GRAPH ?g {
          <${this.uri}> ext:deltaUntil ${sparqlEscapeDateTime(this.latestDelta)} .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);

  }

  /**
   * Persists the given status as task status in the triple store
   *
   * @param status {string} URI of the task status
   * @private
   */
  async persistStatus(status) {
    this.status = status;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> adms:status ?status .
        }
      }
    `);

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      INSERT {
        GRAPH ?g {
          <${this.uri}> adms:status <${this.status}> .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);
  }
}

/**
 * Insert a new sync task in the store to consume delta's if there isn't one scheduled yet.
 * The timestamp from which delta's need to be consumed is determined at the start of the task execution.
 *
 * @public
*/
async function scheduleSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s WHERE {
      ?s a ext:SyncTask ;
        adms:status <${TASK_NOT_STARTED_STATUS}> .
    } LIMIT 1
  `);

  if (result.results.bindings.length) {
    console.log(`There is already a sync task scheduled to ingest delta files. No need to create a new task.`);
  }
  else {
    await createSyncTask();
  }
}

async function createSyncTask(latestDeltaTimestamp, status = TASK_NOT_STARTED_STATUS) {
  const uuid = mu.uuid();
  const uri = `http://lblod.data.gift/mandatendatabank-consumer-sync-tasks/${uuid}`;

  let latestDeltaTimestampTriple = '';

  if(latestDeltaTimestamp){
    latestDeltaTimestampTriple = `${sparqlEscapeUri(uri)} ext:deltaUntil ${sparqlEscapeDateTime(latestDeltaTimestamp)}`;
  }

  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    INSERT DATA {
      GRAPH <${JOBS_GRAPH}> {
        <${uri}> a ext:SyncTask ;
          mu:uuid "${uuid}" ;
          adms:status ${sparqlEscapeUri(status)} ;
          dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)} ;
          dct:created ${sparqlEscapeDateTime(new Date())} .

        ${latestDeltaTimestampTriple}
      }
    }
  `);
  console.log(`Scheduled new sync task <${uri}> to ingest delta files`);
}

/**
 * Get the next sync task with the earliest creation date that has not started yet
 *
 * @public
 */
async function getNextSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s ?created ?deltaUntil WHERE {
      ?s a ext:SyncTask ;
        adms:status ${sparqlEscapeUri(TASK_NOT_STARTED_STATUS)} ;
        dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)} ;
        dct:created ?created .
    } ORDER BY ?created LIMIT 1
  `);

  if (result.results.bindings.length) {
    const syncTaskData = result.results.bindings[0];

    console.log(
        'Getting the timestamp of the latest successfully ingested delta or dump file. This will be used as starting point for consumption.');
    let latestDeltaTimestamp = await getLatestDeltaTimestamp();

    if (!latestDeltaTimestamp) {
      console.log(`It seems to be the first time we will consume delta's. No delta's have been consumed before.`);
      if (START_FROM_DELTA_TIMESTAMP) {
        console.log(`Service is configured to start consuming delta's since ${START_FROM_DELTA_TIMESTAMP}`);
        latestDeltaTimestamp = new Date(Date.parse(START_FROM_DELTA_TIMESTAMP));
      } else {
        throw 'No previous delta file found and no dump file provided, unable to set a starting date for the ingestion.';
      }
    }

    return new SyncTask({
      uri: syncTaskData['s'].value,
      status: TASK_NOT_STARTED_STATUS,
      since: latestDeltaTimestamp,
      created: new Date(Date.parse(syncTaskData['created'].value))
    });
  } else {
    return null;
  }
}

/**
 * Get the URI of the currently running sync task.
 * Null if no task is running.
 *
 * @public
 */
async function getRunningSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s WHERE {
      ?s a ext:SyncTask ;
        dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)} ;
        adms:status <${TASK_ONGOING_STATUS}> .
    } ORDER BY ?created LIMIT 1
  `);

  return result.results.bindings.length ? {uri: result.results.bindings[0]['s']} : null;
}

/**
 * Update the status of a given task to "failed".
 *
 * @public
 */
async function setTaskFailedStatus(uri) {
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    DELETE WHERE {
      GRAPH ?g {
        <${uri}> adms:status ?status .
      }
    }
  `);

  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    INSERT {
      GRAPH ?g {
        <${uri}> adms:status <${TASK_FAILED_STATUS}> .
      }
    } WHERE {
      GRAPH ?g {
        <${uri}> a ext:SyncTask .
      }
    }
  `);
}

/**
 * Get the latest timestamp of a successfully ingested delta file.
 * Even on failed tasks, we're sure ext:deltaUntil reflects the latest
 * successfully ingested delta file.
 *
 * @private
 */
async function getLatestDeltaTimestamp() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?latestDelta WHERE {
      ?s a ext:SyncTask ;
        dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)} ;
        ext:deltaUntil ?latestDelta .
    } ORDER BY DESC(?latestDelta) LIMIT 1
  `);

  if (result.results.bindings.length) {
    const syncTask = result.results.bindings[0];
    return new Date(Date.parse(syncTask['latestDelta'].value));
  } else {
    return null;
  }
}

/**
 * Get a list of produced delta files since a specific datetime
 *
 * @param {Date} since Datetime as of when to fetch delta files
 * @method getUnconsumedFiles
 * @public
 */
async function getUnconsumedFiles(since) {
  try {
    const response = await fetch(`${SYNC_FILES_ENDPOINT}?since=${since.toISOString()}`, {
      headers: {
        'Accept': 'application/vnd.api+json'
      }
    });
    const json = await response.json();
    return json.data.map(f => new DeltaFile(f));
  } catch (e) {
    console.log(`Unable to retrieve unconsumed files from ${SYNC_FILES_ENDPOINT}`);
    throw e;
  }
}

export default SyncTask;
export {
  scheduleSyncTask,
  createSyncTask,
  getNextSyncTask,
  getRunningSyncTask,
  setTaskFailedStatus,
};
