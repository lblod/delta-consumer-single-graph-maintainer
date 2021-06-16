import { uuid, update, sparqlEscapeUri, sparqlEscapeString } from 'mu';
import {
  SERVICE_NAME,
  ERROR_TYPE,
  DELTA_ERROR_TYPE,
  JOBS_GRAPH,
  ERROR_URI_PREFIX,
} from '../config.js';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function exponentialGrowth(initial, rate, interval) {
  return Math.round((initial * Math.pow(1 + rate, interval)));
}

export async function storeError(errorMsg) {
  const id = uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX oslc: <http://open-services.net/ns/core#>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
        ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
          mu:uuid ${sparqlEscapeString(id)} ;
          oslc:message ${sparqlEscapeString('[' + SERVICE_NAME + '] ' + errorMsg)} .
      }
    }
  `;

  await update(queryError);
}

export {
  sleep,
  exponentialGrowth
};