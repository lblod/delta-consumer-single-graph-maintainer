import { uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
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

// Globals
let METACOUNTER = 0;

export function exponentialGrowth(initial, rate, interval) {
  return Math.round((initial * Math.pow(1 + rate, interval)));
}

/**
 * Splits an array into two parts, a part that passes and a part that fails a predicate function.
 *
 * @public
 * @function partition
 * @param {Array} arr - Array to be partitioned
 * @param {Function} fn - Function that accepts single argument: an element of the array, and should return a truthy or falsy value.
 * @returns {Object} Object that contains keys passes and fails, each representing an array with elemets that pass or fail the predicate respectively
 */
export function partition(arr, fn) {
  let passes = [], fails = [];
  arr.forEach((item) => (fn(item) ? passes : fails).push(item));
  return { passes, fails };
}

/**
 * Transform an array of triples to a string of statements to use in a SPARQL query
 *
 * @public
 * @function toStatements
 * @param {Array} - triples Array of triples to convert to a string
 * @returns {String} String that can be used in a SPARQL query
 */
export function toStatements(triples) {
  return triples.map(t => {
    const subject   = escapeRDFTerm(t.subject);
    const predicate = escapeRDFTerm(t.predicate);
    const object    = escapeRDFTerm(t.object);
    return `${subject} ${predicate} ${object} . `;
  }).join('\n');
}

/**
 * This transforms a JSON binding object in SPARQL result format to a string that can be used in a SPARQL query
 *
 * @public
 * @function escapeRDFTerm
 * @param {Object} rdfTerm - Object of the form { value: "...", type: "..." [...] }
 * @returns {String} String representation of the RDF term in SPARQL syntax
 */
export function escapeRDFTerm(rdfTerm) {
  const { type, value, datatype, "xml:lang":lang } = rdfTerm;
  switch (type) {
    case "uri":
      return sparqlEscapeUri(value);
    case "typed-literal":
    case "literal":
      if (datatype)
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype)}`;
      if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    default:
      return sparqlEscapeString(value);
  }
}

export function bodyArrayToMeta(body) {
  if (METACOUNTER >= 100000) METACOUNTER = 0;
  return body.map(i => tripleArrayToMeta(i)).join("");
}

export function tripleArrayToMeta(triple, counterOverride) {
  let counter;
  if (counterOverride) {
    counter = counterOverride;
  }
  else {
    counter = METACOUNTER;
    METACOUNTER++;
  }

  return `
      ?meta${counter}
        ext:subject ${triple[0]} ;
        ext:predicate ${triple[1]} ;
        ext:object ${triple[2]} .`;
}

export function tripleArrayToNewMeta(triple) {
  const muuuid = uuid().toString();
  const uri = "http://mu.semte.ch/vocabularies/ext/".concat(muuuid);
  return `
      ${sparqlEscapeUri(uri)}
        ext:subject ${triple[0]} ;
        ext:predicate ${triple[1]} ;
        ext:object ${triple[2]} .`;
}

export function tripleToMetaTriples(triple) {
  const muuuid = uuid().toString();
  const uri = "http://mu.semte.ch/vocabularies/ext/".concat(muuuid);
  return [
    {
      subject: {
        value: uri,
        type: "uri"
      },
      predicate: {
        value: "http://mu.semte.ch/vocabularies/ext/subject",
        type: "uri"
      },
      object: triple.subject
    },
    {
      subject: {
        value: uri,
        type: "uri"
      },
      predicate: {
        value: "http://mu.semte.ch/vocabularies/ext/predicate",
        type: "uri"
      },
      object: triple.predicate
    },
    {
      subject: {
        value: uri,
        type: "uri"
      },
      predicate: {
        value: "http://mu.semte.ch/vocabularies/ext/object",
        type: "uri"
      },
      object: triple.object
    },
    {
      subject: {
        value: uri,
        type: "uri"
      },
      predicate: {
        value: "http://mu.semte.ch/vocabularies/ext/graph",
        type: "uri"
      },
      object: triple.graph
    }
  ];
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
  return id;
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

    SELECT ?jobUri {
      GRAPH ?g {
        ?jobUri a ${sparqlEscapeUri(JOB_TYPE)}.
        ?jobUri task:operation ${sparqlEscapeUri(jobOperationUri)}.
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
        BIND(${sparqlEscapeUri(job.jobUri)} as ?job)
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
  const jobId = uuid();
  const jobUri = JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)}{
        ${sparqlEscapeUri(jobUri)} a ${sparqlEscapeUri(JOB_TYPE)};
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

  return jobUri;
}
