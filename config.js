// CONFIGURATION
export const SYNC_FILES_PATH = process.env._SYNC_FILES_PATH || '/sync/files';
export const DOWNLOAD_FILE_PATH = process.env._DOWNLOAD_FILE_PATH || '/files/:id/download';
export const SYNC_DATASET_PATH = process.env._SYNC_DATASET_PATH || '/datasets';
export const START_FROM_DELTA_TIMESTAMP = process.env._START_FROM_DELTA_TIMESTAMP;
export const DELTA_FILE_FOLDER = process.env._DELTA_FILE_FOLDER || '/tmp/';
export const KEEP_DELTA_FILES = process.env._KEEP_DELTA_FILES == 'true';
export const DISABLE_DELTA_INGEST = process.env._DISABLE_DELTA_INGEST == 'true' ? true : false;
export const DISABLE_INITIAL_SYNC = process.env._DISABLE_INITIAL_SYNC == 'true' ? true : false;
export const WAIT_FOR_INITIAL_SYNC = process.env._WAIT_FOR_INITIAL_SYNC == 'false'? false: true;
export const DUMPFILE_FOLDER = process.env._DUMPFILE_FOLDER || 'consumer/deltas';
export const CRON_PATTERN_DELTA_SYNC = process.env._CRON_PATTERN_DELTA_SYNC || '0 * * * * *'; // every minute

// GRAPHS
export const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';

// MANDATORY SIMPLE
if(!process.env._SYNC_BASE_URL)
  throw `Expected '_SYNC_BASE_URL' to be provided.`;
export const SYNC_BASE_URL = process.env._SYNC_BASE_URL;

if(!process.env._SERVICE_NAME)
  throw `Expected '_SERVICE_NAME' to be provided.`;
export const SERVICE_NAME = process.env._SERVICE_NAME;

if(!process.env._JOB_CREATOR_URI)
  throw `Expected '_JOB_CREATOR_URI' to be provided.`;
export const JOB_CREATOR_URI = process.env._JOB_CREATOR_URI;

if(!process.env._DELTA_SYNC_JOB_OPERATION)
  throw `Expected '_DELTA_SYNC_JOB_OPERATION' to be provided.`;
export const DELTA_SYNC_JOB_OPERATION = process.env._DELTA_SYNC_JOB_OPERATION;

// MANDATARY CONDITIONAL
if(!process.env._SYNC_DATASET_SUBJECT && (WAIT_FOR_INITIAL_SYNC || !DISABLE_INITIAL_SYNC))
  throw `Expected '_SYNC_DATASET_SUBJECT' to be provided by default.`;
export const SYNC_DATASET_SUBJECT = process.env._SYNC_DATASET_SUBJECT;

if(!process.env._INITIAL_SYNC_JOB_OPERATION && (WAIT_FOR_INITIAL_SYNC || !DISABLE_INITIAL_SYNC))
  throw `Expected '_INITIAL_SYNC_JOB_OPERATION' to be provided by default.`;
export const INITIAL_SYNC_JOB_OPERATION = process.env._INITIAL_SYNC_JOB_OPERATION;


// COMPOSED VARIABLES
export const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
export const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
export const SYNC_DATASET_ENDPOINT = `${SYNC_BASE_URL}${SYNC_DATASET_PATH}`;
