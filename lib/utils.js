import * as mu from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

import {
  SERVICE_NAME,
  ERROR_TYPE,
  DELTA_ERROR_TYPE,
  JOBS_GRAPH,
  ERROR_URI_PREFIX,
  PREFIXES,
  JOB_TYPE,
  JOB_URI_PREFIX,
  JOB_CREATOR_URI,
  STATUS_BUSY
} from '../config.js';

export function exponentialGrowth(initial, rate, interval) {
  return Math.round((initial * Math.pow(1 + rate, interval)));
}

export function partition(arr, fn) {
  let passes = [], fails = [];
  arr.forEach((item) => (fn(item) ? passes : fails).push(item));
  return { passes, fails };
}

/**
 * Transform an array of triples to a string of statements to use in a SPARQL query
 *
 * @param {Array} triples Array of triples to convert
 * @method toStatements
 * @private
 */
export function toStatements(triples) {
  return triples.map(t => {
    const subject   = escapeRDFTerm(t.subject);
    const predicate = escapeRDFTerm(t.predicate);
    const object    = escapeRDFTerm(t.object);
    return `${subject} ${predicate} ${object} . `;
  }).join('\n');
}

export function escapeRDFTerm(rdfTerm) {
  const { type, value, datatype, "xml:lang":lang } = rdfTerm;
  switch (type) {
    case "uri":
      return mu.sparqlEscapeUri(value);
    case "typed-literal":
    case "literal":
      if (datatype)
        return `${mu.sparqlEscapeString(value)}^^${mu.sparqlEscapeUri(datatype)}`;
      if (lang)
        return `${mu.sparqlEscapeString(value)}@${lang}`;
      else
        return `${mu.sparqlEscapeString(value)}`;
    default:
      return mu.sparqlEscapeString(value);
  }
}

export async function storeError(errorMsg) {
  const id = mu.uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX oslc: <http://open-services.net/ns/core#>

    INSERT DATA {
      GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} a ${mu.sparqlEscapeUri(ERROR_TYPE)}, ${mu.sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
          mu:uuid ${mu.sparqlEscapeString(id)} ;
          oslc:message ${mu.sparqlEscapeString('[' + SERVICE_NAME + '] ' + errorMsg)} .
      }
    }
  `;

  await update(queryError);
}

export async function getJobs(jobOperationUri, statusFilterIn = [], statusFilterNotIn = []){
  let statusFilterInString = '';

  if(statusFilterIn.length){
    const escapedFilters = statusFilterIn.map(s => mu.sparqlEscapeUri(s)).join(', ');
    statusFilterInString = `FILTER(?status IN (${escapedFilters}))`;
  }

  let statusFilterNotInString = '';
  if(statusFilterNotIn.length){
    const escapedFilters = statusFilterNotIn.map(s => mu.sparqlEscapeUri(s)).join(', ');
    statusFilterNotInString = `FILTER(?status NOT IN (${escapedFilters}))`;
  }

  const queryIsActive = `
    ${PREFIXES}

    SELECT ?jobUri {
      GRAPH ?g {
        ?jobUri a ${mu.sparqlEscapeUri(JOB_TYPE)}.
        ?jobUri task:operation ${mu.sparqlEscapeUri(jobOperationUri)}.
        ?jobUri adms:status ?status.

        ${statusFilterInString}
        ${statusFilterNotInString}
      }
    }
  `;
  const result = await query(queryIsActive);
  return result.results.bindings.length ? result.results.bindings.map( r => { return { jobUri: r.jobUri.value }; }) : [];
}

export async function cleanupJobs(jobs){
  for(const job of jobs){
    const cleanupQuery = `
      ${PREFIXES}

      DELETE {
        GRAPH ?g {
          ?job ?jobP ?jobO.
          ?task ?taskP ?taskO.
        }
      }
      WHERE {
        BIND(${mu.sparqlEscapeUri(job.jobUri)} as ?job)
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
}


export async function createJob(jobOperationUri){
  const jobId = mu.uuid();
  const jobUri = JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)}{
        ${mu.sparqlEscapeUri(jobUri)} a ${mu.sparqlEscapeUri(JOB_TYPE)};
          mu:uuid ${mu.sparqlEscapeString(jobId)};
          dct:creator ${mu.sparqlEscapeUri(JOB_CREATOR_URI)};
          adms:status ${mu.sparqlEscapeUri(STATUS_BUSY)};
          dct:created ${mu.sparqlEscapeDateTime(created)};
          dct:modified ${mu.sparqlEscapeDateTime(created)};
          task:operation ${mu.sparqlEscapeUri(jobOperationUri)}.
      }
    }
  `;

  await update(createJobQuery);

  return jobUri;
}
