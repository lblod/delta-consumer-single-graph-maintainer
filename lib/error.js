import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { DELTA_ERROR_TYPE, ERROR_TYPE, ERROR_URI_PREFIX, PREFIXES } from './constants';

export async function createJobError(jobsGraph, subject, errorMsg) {
  const id = uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(jobsGraph)} {
        ${sparqlEscapeUri(uri)}
          a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
          mu:uuid ${sparqlEscapeString(id)} ;
          oslc:message ${sparqlEscapeString(errorMsg)} .
        ${sparqlEscapeUri(subject)} task:error ${sparqlEscapeUri(uri)} .
      }
    }
  `;
  await update(queryError);
}

export async function createError(jobsGraph, serviceName, errorMsg) {
  const id = uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX oslc: <http://open-services.net/ns/core#>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(jobsGraph)} {
        ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
          mu:uuid ${sparqlEscapeString(id)} ;
          oslc:message ${sparqlEscapeString('[' + serviceName + '] ' + errorMsg)} .
      }
    }
  `;

  await update(queryError);
}
