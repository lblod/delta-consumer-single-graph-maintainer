const { BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES,
        DIRECT_DATABASE_ENDPOINT,
        MU_CALL_SCOPE_ID_INITIAL_SYNC,
        BATCH_SIZE,
        MAX_DB_RETRY_ATTEMPTS,
        SLEEP_BETWEEN_BATCHES,
        SLEEP_TIME_AFTER_FAILED_DB_OPERATION,
        INGEST_GRAPH
      } = require('./config');
const { batchedDbUpdate } = require('./utils');
const endpoint = BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES ? DIRECT_DATABASE_ENDPOINT : process.env.MU_SPARQL_ENDPOINT;


/**
 * Dispatch the fetched information to a target graph.
 * @param { mu, muAuthSudo } lib - The provided libraries from the host service.
 * @param { changeSets: { deleteTermObjects, insertTermObjects } } data - The fetched changes sets, which objects of serialized Terms
 *          [ {
 *              graph: "<http://foo>",
 *              subject: "<http://bar>",
 *              predicate: "<http://baz>",
 *              object: "<http://boom>^^<http://datatype>"
 *            }
 *         ]
 * @return {void} Nothing
 */
async function dispatch(lib, data){
  const { mu, muAuthSudo } = lib;
  const { changeSets } =  data;

  for (let { deleteTermObjects, insertTermObjects } of changeSets) {
    const deleteStatements = deleteTermObjects.map(o => `${o.subject} ${o.predicate} ${o.object}.`);
    await batchedDbUpdate(
      muAuthSudo.updateSudo,
      INGEST_GRAPH,
      deleteStatements,
      { },
      process.env.MU_SPARQL_ENDPOINT, //Note: this is the default endpoint through auth
      BATCH_SIZE,
      MAX_DB_RETRY_ATTEMPTS,
      SLEEP_BETWEEN_BATCHES,
      SLEEP_TIME_AFTER_FAILED_DB_OPERATION,
      "DELETE"
     );
    const insertStatements = insertTermObjects.map(o => `${o.subject} ${o.predicate} ${o.object}.`);
    await batchedDbUpdate(
      muAuthSudo.updateSudo,
      INGEST_GRAPH,
      insertStatements,
      { },
      process.env.MU_SPARQL_ENDPOINT, //Note: this is the default endpoint through auth
      BATCH_SIZE,
      MAX_DB_RETRY_ATTEMPTS,
      SLEEP_BETWEEN_BATCHES,
      SLEEP_TIME_AFTER_FAILED_DB_OPERATION,
      "INSERT"
     );
  }
}

module.exports = {
  dispatch
};
