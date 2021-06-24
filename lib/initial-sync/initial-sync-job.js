import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';

import {
  JOB_URI_PREFIX,
  PREFIXES,
  JOB_TYPE,
  JOBS_GRAPH,
  JOB_CREATOR_URI,
  STATUS_BUSY,
  STATUS_FAILED,
  INITIAL_SYNC_JOB_OPERATION
} from '../../config';

class InitialSyncJob {
  constructor({ uri, created, status }) {
    /** Uri of the job */
    this.uri = uri;

    /**
     * Datetime as Data object when the job was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the job as stored in the triplestore
    */
    this.status = status;
  }


  /**
   * Close the job with a failure status
   *
   * @public
  */
  async closeWithFailure() {
    await this.updateStatus(STATUS_FAILED);
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
* Insert an initial sync job in the store to consume a dump file.
*
* @public
*/
async function scheduleInitialSyncJob() {
  const job = await createJob(INITIAL_SYNC_JOB_OPERATION);
  console.log(`Scheduled initial sync job <${job.uri}>`);
  return job;
}

/**
* Gets the latest initial sync job in the store if any.
*
* @public
*/
async function getLatestInitialSyncJob() {
  const result = await query(`
    ${PREFIXES}
    SELECT ?s ?status ?created WHERE {
      ?s a ${sparqlEscapeUri(JOB_TYPE)} ;
        adms:status ?status ;
        task:operation ${sparqlEscapeUri(INITIAL_SYNC_JOB_OPERATION)} ;
        dct:created ?created ;
        dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)} .
    }
    ORDER BY DESC(?created)
    LIMIT 1
  `);

  if (result.results.bindings.length) {
    return new InitialSyncJob({
      uri: result.results.bindings[0].s.value,
      status: result.results.bindings[0].status.value,
      created: result.results.bindings[0].created.value
    });
  } else {
    return null;
  }
}

async function createJob(jobOperationUri){
  const jobId = uuid();
  const jobUri = JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)}{
        ${sparqlEscapeUri(jobUri)}
          a ${sparqlEscapeUri(JOB_TYPE)};
          mu:uuid ${sparqlEscapeString(jobId)};
          dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)};
          adms:status ${sparqlEscapeUri(STATUS_BUSY)};
          dct:created ${sparqlEscapeDateTime(created)};
          dct:modified ${sparqlEscapeDateTime(created)};
          task:operation ${sparqlEscapeUri(jobOperationUri)}.
      }
    }
  `;

  await update(createJobQuery);

  return new InitialSyncJob({
    uri: jobUri,
    status: STATUS_BUSY,
    created: created
  });
}

export default InitialSyncJob;
export {
  getLatestInitialSyncJob,
  scheduleInitialSyncJob
};
