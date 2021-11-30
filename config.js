// CONFIGURATION
export const SYNC_FILES_PATH = process.env.DCR_SYNC_FILES_PATH || '/sync/files';
export const DOWNLOAD_FILE_PATH = process.env.DCR_DOWNLOAD_FILE_PATH || '/files/:id/download';
export const SYNC_DATASET_PATH = process.env.DCR_SYNC_DATASET_PATH || '/datasets';
export const START_FROM_DELTA_TIMESTAMP = process.env.DCR_START_FROM_DELTA_TIMESTAMP;
export const DELTA_FILE_FOLDER = process.env.DCR_DELTA_FILE_FOLDER || '/tmp/';
export const KEEP_DELTA_FILES = process.env.DCR_KEEP_DELTA_FILES == 'true';
export const DISABLE_DELTA_INGEST = process.env.DCR_DISABLE_DELTA_INGEST == 'true' ? true : false;
export const DISABLE_INITIAL_SYNC = process.env.DCR_DISABLE_INITIAL_SYNC == 'true' ? true : false;
export const WAIT_FOR_INITIAL_SYNC = process.env.DCR_WAIT_FOR_INITIAL_SYNC == 'false'? false: true;
export const DUMPFILE_FOLDER = process.env.DCR_DUMPFILE_FOLDER || 'consumer/deltas';
export const CRON_PATTERN_DELTA_SYNC = process.env.DCR_CRON_PATTERN_DELTA_SYNC || '0 * * * * *'; // every minute

// GRAPHS
export const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';

// MANDATORY SIMPLE
if(!process.env.DCR_SYNC_BASE_URL)
  throw `Expected 'DCR_SYNC_BASE_URL' to be provided.`;
export const SYNC_BASE_URL = process.env.DCR_SYNC_BASE_URL;

if(!process.env.DCR_SERVICE_NAME)
  throw `Expected 'DCR_SERVICE_NAME' to be provided.`;
export const SERVICE_NAME = process.env.DCR_SERVICE_NAME;

if(!process.env.DCR_JOB_CREATOR_URI)
  throw `Expected 'DCR_JOB_CREATOR_URI' to be provided.`;
export const JOB_CREATOR_URI = process.env.DCR_JOB_CREATOR_URI;

if(!process.env.DCR_DELTA_SYNC_JOB_OPERATION)
  throw `Expected 'DCR_DELTA_SYNC_JOB_OPERATION' to be provided.`;
export const DELTA_SYNC_JOB_OPERATION = process.env.DCR_DELTA_SYNC_JOB_OPERATION;

// MANDATARY CONDITIONAL
if(!process.env.DCR_SYNC_DATASET_SUBJECT && (WAIT_FOR_INITIAL_SYNC || !DISABLE_INITIAL_SYNC))
  throw `Expected 'DCR_SYNC_DATASET_SUBJECT' to be provided by default.`;
export const SYNC_DATASET_SUBJECT = process.env.DCR_SYNC_DATASET_SUBJECT;

if(!process.env.DCR_INITIAL_SYNC_JOB_OPERATION && (WAIT_FOR_INITIAL_SYNC || !DISABLE_INITIAL_SYNC))
  throw `Expected 'DCR_INITIAL_SYNC_JOB_OPERATION' to be provided by default.`;
export const INITIAL_SYNC_JOB_OPERATION = process.env.DCR_INITIAL_SYNC_JOB_OPERATION;


// COMPOSED VARIABLES
export const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
export const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
export const SYNC_DATASET_ENDPOINT = `${SYNC_BASE_URL}${SYNC_DATASET_PATH}`;
