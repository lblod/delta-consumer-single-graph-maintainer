const { BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES,
        DIRECT_DATABASE_ENDPOINT,
        MU_CALL_SCOPE_ID_INITIAL_SYNC,
        BATCH_SIZE,
        MAX_DB_RETRY_ATTEMPTS,
        SLEEP_BETWEEN_BATCHES,
        SLEEP_TIME_AFTER_FAILED_DB_OPERATION,
        INGEST_GRAPH
      } = require('./config');
const { batchedDbUpdate } = require('./utils');;
const endpoint = BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES ? DIRECT_DATABASE_ENDPOINT : process.env.MU_SPARQL_ENDPOINT;

async function dispatch(lib, data){
  const { mu, muAuthSudo } = lib;
  const { changeSets } =  data;

  for (let { inserts, deletes } of changeSets) {
    const deleteStatements = deletes.map(o => `${o.subject} ${o.predicate} ${o.object}.`);
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
    const insertStatements = inserts.map(o => `${o.subject} ${o.predicate} ${o.object}.`);
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
