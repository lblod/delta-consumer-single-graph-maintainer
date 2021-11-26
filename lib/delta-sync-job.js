import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { INITIAL_SYNC_JOB_OPERATION, JOB_CREATOR_URI, START_FROM_DELTA_TIMESTAMP, DELTA_SYNC_JOB_OPERATION } from '../config';
import { CONTAINER_TYPE, DELTA_SYNC_TASK_OPERATION, JOB_TYPE, PREFIXES, STATUS_SUCCESS, TASK_TYPE } from './constants';
import { parseResult } from './utils';


export async function calculateLatestDeltaTimestamp() {
  const timestamp = await loadTimestampFromJob();
  if(timestamp) {
    return timestamp.deltaTimestamp;
  }
  else {
    return loadTimestampFromConfig();
  }
}

async function loadTimestampFromJob(){
  const queryStr = `
    ${PREFIXES}
    SELECT DISTINCT ?deltaTimestamp WHERE {
      ?job a ${sparqlEscapeUri(JOB_TYPE)} ;
        task:operation ?operation;
        dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)}.

      ?task a ${ sparqlEscapeUri(TASK_TYPE) };
        dct:isPartOf ?job;
        adms:status ${sparqlEscapeUri(STATUS_SUCCESS)};
        dct:modified ?modified;
        task:operation ${sparqlEscapeUri(DELTA_SYNC_TASK_OPERATION)} ;
        task:resultsContainer ?resultsContainer.

      ?resultsContainer a ${sparqlEscapeUri(CONTAINER_TYPE)};
        dct:subject <http://redpencil.data.gift/id/concept/DeltaSync/DeltafileInfo>;
        ext:hasDeltafileTimestamp ?deltaTimestamp.

       VALUES ?operation {
         ${sparqlEscapeUri(INITIAL_SYNC_JOB_OPERATION)}
         ${sparqlEscapeUri(DELTA_SYNC_JOB_OPERATION)}
       }
    }
    ORDER BY DESC(?deltaTimestamp)
    LIMIT 1
  `;
  return parseResult(await query(queryStr))[0];
}

function loadTimestampFromConfig(){
  console.log(`It seems to be the first time we will consume delta's. No delta's have been consumed before.`);
  if (START_FROM_DELTA_TIMESTAMP) {
    console.log(`Service is configured to start consuming delta's since ${START_FROM_DELTA_TIMESTAMP}`);
    return new Date(Date.parse(START_FROM_DELTA_TIMESTAMP));
  }
  else {
    throw 'No previous delta file found and no START_FROM_DELTA_TIMESTAMP provided, unable to set a starting date for the ingestion.';
  }
}
