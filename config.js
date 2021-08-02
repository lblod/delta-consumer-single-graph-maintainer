// MANDATORY

if(!process.env.SERVICE_NAME)
  throw `Expected 'SERVICE_NAME' to be provided.`;
export const SERVICE_NAME = process.env.SERVICE_NAME;

if(!process.env.SYNC_DATASET_SUBJECT)
  throw `Expected 'SYNC_DATASET_SUBJECT' to be provided.`;
export const SYNC_DATASET_SUBJECT = process.env.SYNC_DATASET_SUBJECT;

if(!process.env.JOB_CREATOR_URI)
  throw `Expected 'JOB_CREATOR_URI' to be provided.`;
export const JOB_CREATOR_URI = process.env.JOB_CREATOR_URI;

if(!process.env.INITIAL_SYNC_JOB_OPERATION)
  throw `Expected 'INITIAL_SYNC_JOB_OPERATION' to be provided.`;
export const INITIAL_SYNC_JOB_OPERATION = process.env.INITIAL_SYNC_JOB_OPERATION;

// CONFIGURATION

export const SYNC_BASE_URL = process.env.SYNC_BASE_URL;
export const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || '/sync/files';
export const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || '/files/:id/download';
export const SYNC_DATASET_PATH = process.env.SYNC_DATASET_PATH || '/datasets';
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
export const START_FROM_DELTA_TIMESTAMP = process.env.START_FROM_DELTA_TIMESTAMP;
export const DELTA_FILE_FOLDER = process.env.DELTA_FILE_FOLDER || '/tmp/';
export const KEEP_DELTA_FILES = process.env.KEEP_DELTA_FILES == 'true';
export const DISABLE_DELTA_INGEST = process.env.DISABLE_DELTA_INGEST == 'true' ? true : false;
export const DISABLE_INITIAL_SYNC = process.env.DISABLE_INITIAL_SYNC == 'true' ? true : false;
export const WAIT_FOR_INITIAL_SYNC = process.env.WAIT_FOR_INITIAL_SYNC == 'false'? false: true;
export const DUMPFILE_FOLDER = process.env.DUMPFILE_FOLDER || 'consumer/deltas';
export const MU_CALL_SCOPE_ID_INITIAL_SYNC = process.env.MU_CALL_SCOPE_ID_INITIAL_SYNC || 'http://redpencil.data.gift/id/concept/muScope/deltas/consumer/initialSync';
export const CRON_PATTERN_DELTA_SYNC = process.env.CRON_PATTERN_DELTA_SYNC || '0 * * * * *'; // every minute
export const BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES = process.env.BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES == 'true' ? true : false;
export const DIRECT_DATABASE_ENDPOINT = process.env.DIRECT_DATABASE_ENDPOINT || 'http://virtuoso:8890/sparql';
export const MAX_DB_RETRY_ATTEMPTS = parseInt(process.env.MAX_DB_RETRY_ATTEMPTS || 5);
export const SLEEP_TIME_AFTER_FAILED_DB_OPERATION = parseInt(process.env.SLEEP_TIME_AFTER_FAILED_DB_OPERATION || 60000);

// GRAPHS
export const INGEST_GRAPH = process.env.INGEST_GRAPH || `http://mu.semte.ch/graphs/public`;
export const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';

// JOBS & TASKS
export const JOB_URI_PREFIX = 'http://redpencil.data.gift/id/job/';
export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_URI_PREFIX = 'http://redpencil.data.gift/id/task/';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const INITIAL_SYNC_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/consumer/initialSyncing';

// INTERNAL SYNC TASKS

export const TASK_NOT_STARTED_STATUS = `http://lblod.data.gift/sync-task-statuses/not-started`;
export const TASK_ONGOING_STATUS = `http://lblod.data.gift/sync-task-statuses/ongoing`;
export const TASK_SUCCESS_STATUS = `http://lblod.data.gift/sync-task-statuses/success`;
export const TASK_FAILED_STATUS = `http://lblod.data.gift/sync-task-statuses/failure`;

// STATUS

export const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
export const STATUS_CANCELED = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';
export const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';

// ERRORS

export const ERROR_TYPE= 'http://open-services.net/ns/core#Error';
export const DELTA_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/deltas/Error';
export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';

// STATICS
export const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
export const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
export const SYNC_DATASET_ENDPOINT = `${SYNC_BASE_URL}${SYNC_DATASET_PATH}`;

export const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
`;
