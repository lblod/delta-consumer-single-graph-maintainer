import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';


import {
  INGEST_GRAPH,
  BATCH_SIZE,
  TASK_URI_PREFIX,
  PREFIXES,
  TASK_TYPE,
  JOBS_GRAPH,
  STATUS_SCHEDULED,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
  ERROR_URI_PREFIX,
  ERROR_TYPE,
  DELTA_ERROR_TYPE,
  INITIAL_SYNC_TASK_OPERATION,
  MU_CALL_SCOPE_ID_INITIAL_SYNC,
  BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES,
  DIRECT_DATABASE_ENDPOINT,
  MAX_DB_RETRY_ATTEMPTS,
  SLEEP_TIME_AFTER_FAILED_DB_OPERATION
} from '../../config';

class InitialSyncTask {
  constructor({ uri, created, status }) {
    /** Uri of the sync task */
    this.uri = uri;

    /**
     * Datetime as Data object when the task was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the sync task as stored in the triplestore
    */
    this.status = status;

    /**
     * The dump file to be ingested for this task
     *
     * @type DumpFile
    */
    this.dumpFile = null;
  }

  /**
   * Execute the initial sync task
   * I.e. consume the dump file by chuncks
   *
   * @public
  */
  async execute() {
    try {
      if (this.dumpFile) {
        await this.updateStatus(STATUS_BUSY);
        const endpoint = BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES ? DIRECT_DATABASE_ENDPOINT : process.env.MU_SPARQL_ENDPOINT;

        if(BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES){
          console.warn(`Service configured to skip MU_AUTH!`);
        }
        console.log(`Using ${endpoint} to insert triples`);

        await insertTriples(await this.dumpFile.loadTripleStream(), { 'mu-call-scope-id': MU_CALL_SCOPE_ID_INITIAL_SYNC }, endpoint);
        await this.updateStatus(STATUS_SUCCESS);
      }
      else {
        console.log(`No dump file to consume. Is the producing stack ready?`);
        throw new Error('No dump file found.');
      }
    }
    catch (e) {
      console.log(`Something went wrong while consuming the files`);
      console.log(e);
      throw(e);
    }
  }

  /**
   * Close the sync task with a failure status
   *
   * @public
  */
  async closeWithFailure(error) {
    await this.updateStatus(STATUS_FAILED);
    await this.storeError(error.message || error);
  }

  async storeError(errorMsg) {
    const id = uuid();
    const uri = ERROR_URI_PREFIX + id;

    const queryError = `
      ${PREFIXES}
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
          ${sparqlEscapeUri(uri)}
            a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
            mu:uuid ${sparqlEscapeString(id)} ;
            oslc:message ${sparqlEscapeString(errorMsg)} .
          ${sparqlEscapeUri(this.uri)} task:error ${sparqlEscapeUri(uri)} .
        }
      }
    `;

    await update(queryError);
  }

  /**
  * Updates the status of the given resource
  */
  async updateStatus(status) {
    this.status = status;

    const q = `
      PREFIX adms: <http://www.w3.org/ns/adms#>

      DELETE {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
      INSERT {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ${sparqlEscapeUri(this.status)} .
        }
      }
      WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
    `;
    await update(q);
  }
}

/**
 * Insert an initial sync job in the store to consume a dump file if no such task exists yet.
 *
 * @public
*/
async function scheduleInitialSyncTask(job) {
  const task = await scheduleTask(job.uri, INITIAL_SYNC_TASK_OPERATION);
  console.log(`Scheduled initial sync task <${task.uri}> to ingest dump file`);
  return task;
}

async function scheduleTask(jobUri, taskOperationUri, taskIndex = "0"){
  const taskId = uuid();
  const taskUri = TASK_URI_PREFIX + `${taskId}`;
  const created = new Date();
  const createTaskQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
        ${sparqlEscapeUri(taskUri)}
          a ${sparqlEscapeUri(TASK_TYPE)};
          mu:uuid ${sparqlEscapeString(taskId)};
          adms:status ${sparqlEscapeUri(STATUS_SCHEDULED)};
          dct:created ${sparqlEscapeDateTime(created)};
          dct:modified ${sparqlEscapeDateTime(created)};
          task:operation ${sparqlEscapeUri(taskOperationUri)};
          task:index ${sparqlEscapeString(taskIndex)};
          dct:isPartOf ${sparqlEscapeUri(jobUri)}.
      }
    }`;

  await update(createTaskQuery);

  return new InitialSyncTask({
    uri: taskUri,
    status: STATUS_SCHEDULED,
    created: created
  });
}

async function insertTriples(tripleStream, extraHeaders, endpoint) {
  let batch = [];

  tripleStream.on('data', async (quad) => {
    batch.push(quad);

    if (batch.length >= BATCH_SIZE) {
      console.log(`Inserting batch of ${batch.length} triples`);
      let oldBatch = batch;
      batch = [];

      await insertBatch(oldBatch, extraHeaders, endpoint);
    }
  });

  tripleStream.on('end', () => {
    if (batch.length > 0) {
      insertBatch(batch, extraHeaders, endpoint);
      batch = [];
    }
  });
}

async function insertBatch(batch, extraHeaders, endpoint) {
  const insertCall = async () => {
    await update(`
      INSERT DATA {
      GRAPH <${INGEST_GRAPH}> {
          ${batch}
        }
      }
    `, extraHeaders, endpoint);
  };

  await dbOperationWithRetry(insertCall);
}

async function dbOperationWithRetry(callback,
                                    attempt = 0,
                                    maxAttempts = MAX_DB_RETRY_ATTEMPTS,
                                    sleepTimeOnFail= SLEEP_TIME_AFTER_FAILED_DB_OPERATION){
  try {
    return await callback();
  }
  catch(e){
    console.log(`Operation failed for ${callback.toString()}, attempt: ${attempt} of ${maxAttempts}`);
    console.log(`Error: ${e}`);
    console.log(`Sleeping ${sleepTimeOnFail} ms`);

    if(attempt >= maxAttempts){
      console.log(`Max attempts reached for ${callback.toString()}, giving up`);
      throw e;
    }

    await new Promise(r => setTimeout(r, sleepTimeOnFail));
    return dbOperationWithRetry(callback, ++attempt, maxAttempts, sleepTimeOnFail);
  }
}

export default InitialSyncTask;
export {
  scheduleInitialSyncTask
};
