import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { JOB_TYPE, JOB_URI_PREFIX, PREFIXES, STATUS_SCHEDULED, STATUS_FAILED } from './constants';
import { parseResult, updateStatus } from './utils';

export async function createJob(jobsGraph,
                                jobOperationUri,
                                jobCreator,
                                status = STATUS_SCHEDULED
                               ){
  const jobId = uuid();
  const jobUri = JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(jobsGraph)}{
        ${sparqlEscapeUri(jobUri)} a ${sparqlEscapeUri(JOB_TYPE)};
          mu:uuid ${sparqlEscapeString(jobId)};
          dct:creator ${sparqlEscapeUri(jobCreator)};
          adms:status ${sparqlEscapeUri(status)};
          dct:created ${sparqlEscapeDateTime(created)};
          dct:modified ${sparqlEscapeDateTime(created)};
          task:operation ${sparqlEscapeUri(jobOperationUri)}.
      }
    }
  `;

  await update(createJobQuery);

  return jobUri;
}

export async function loadJob(subject){
  const queryJob = `
    ${PREFIXES}
    SELECT DISTINCT ?graph ?job ?created ?modified ?creator ?status ?error ?operation WHERE {
     GRAPH ?graph {
       BIND(${sparqlEscapeUri(subject)} AS ?job)
       ?job a ${sparqlEscapeUri(JOB_TYPE)};
         dct:creator ?creator;
         adms:status ?status;
         dct:created ?created;
         task:operation ?operation;
         dct:modified ?modified.
       OPTIONAL { ?job task:error ?error. }
     }
    }
  `;

  const job = parseResult(await query(queryJob))[0];
  if(!job) return null;

  //load has many
  const queryTasks = `
   ${PREFIXES}
   SELECT DISTINCT ?job ?task WHERE {
     GRAPH ?g {
       BIND(${ sparqlEscapeUri(subject) } as ?job)
       ?task dct:isPartOf ?job
      }
    }
  `;

  const tasks = parseResult(await query(queryTasks)).map(row => row.task);
  job.tasks = tasks;

  return job;
}

export async function getLatestJobForOperation(operation, jobCreator, statusFilter = []) {

  let statusFilterInString = '';
  if(statusFilter.length){
    const escapedFilters = statusFilter.map(s => sparqlEscapeUri(s)).join(', ');
    statusFilterInString = `FILTER(?status IN (${escapedFilters}))`;
  }

  const result = await query(`
    ${PREFIXES}
    SELECT DISTINCT ?s ?status ?created WHERE {
      ?s a ${sparqlEscapeUri(JOB_TYPE)} ;
        task:operation ${sparqlEscapeUri(operation)} ;
        dct:creator ${sparqlEscapeUri(jobCreator)} .

      ${statusFilterInString}
    }
    ORDER BY DESC(?created)
    LIMIT 1
  `);

  const job = parseResult(result)[0];
  if(job){
    return loadJob(job.s);
  }
  else return null;
}

export async function getJobs(jobOperationUri, statusFilterIn = [], statusFilterNotIn = []){

  let statusFilterInString = '';
  if(statusFilterIn.length){
    const escapedFilters = statusFilterIn.map(s => sparqlEscapeUri(s)).join(', ');
    statusFilterInString = `FILTER(?status IN (${escapedFilters}))`;
  }

  let statusFilterNotInString = '';
  if(statusFilterNotIn.length){
    const escapedFilters = statusFilterNotIn.map(s => sparqlEscapeUri(s)).join(', ');
    statusFilterNotInString = `FILTER(?status NOT IN (${escapedFilters}))`;
  }

  const queryIsActive = `
    ${PREFIXES}

    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(JOB_TYPE)}.
        ?job task:operation ${sparqlEscapeUri(jobOperationUri)}.
        ?job adms:status ?status.

        ${statusFilterInString}
        ${statusFilterNotInString}
      }
    }
  `;
  const result = await query(queryIsActive);

  const jobs = [];
  for(const job of parseResult(result)){
    jobs.push(await loadJob(job.job));
  }

  return jobs;
}

export async function failJob( job ) {
  let jobObject = await loadJob(job);

  for(const task of jobObject.tasks){
    await updateStatus(task, STATUS_FAILED);
  }
  await updateStatus(jobObject.job, STATUS_FAILED);
}

export async function cleanupJob(job) {
  const cleanupQuery = `
    ${PREFIXES}

    DELETE {
      GRAPH ?g {
        ?job ?jobP ?jobO.
        ?task ?taskP ?taskO.
      }
    }
    WHERE {
      BIND(${sparqlEscapeUri(job)} as ?job)
      GRAPH ?g {
        ?job ?jobP ?jobO.
        OPTIONAL {
          ?task dct:isPartOf ?job.
          ?task ?taskP ?taskO.
        }
      }
    }
  `;
  await update(cleanupQuery);
}
