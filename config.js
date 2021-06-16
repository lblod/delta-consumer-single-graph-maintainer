// CONFIGURATION
const SYNC_BASE_URL = process.env.SYNC_BASE_URL;
const SERVICE_NAME = process.env.SERVICE_NAME;
const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || '/sync/files';
const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || '/files/:id/download';
const INGEST_INTERVAL = process.env.INGEST_INTERVAL || -1;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
const PUBLIC_GRAPH = process.env.PUBLIC_GRAPH || 'http://mu.semte.ch/graphs/public';
const INGEST_GRAPH = process.env.INGEST_GRAPH || `http://mu.semte.ch/graphs/public`;
const START_FROM_DELTA_TIMESTAMP = process.env.START_FROM_DELTA_TIMESTAMP;
const DELTA_FILE_FOLDER = process.env.DELTA_FILE_FOLDER || '/tmp/';
const KEEP_DELTA_FILES = process.env.KEEP_DELTA_FILES == 'true';
const DISABLE_DELTA_INGEST = process.env.DISABLE_DELTA_INGEST == 'true' ? true : false;

if(!SERVICE_NAME) {
  throw "SERVICE_NAME is required. Please provide one.";
}

// ERRORS

const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';
const ERROR_TYPE= 'http://open-services.net/ns/core#Error';
const DELTA_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/deltas/Error';
const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';

// STATICS
const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;

export {
  SERVICE_NAME,
  INGEST_INTERVAL,
  SYNC_BASE_URL,
  SYNC_FILES_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE,
  PUBLIC_GRAPH,
  INGEST_GRAPH,
  START_FROM_DELTA_TIMESTAMP,
  KEEP_DELTA_FILES,
  DELTA_FILE_FOLDER,
  DISABLE_DELTA_INGEST,
  JOBS_GRAPH,
  ERROR_TYPE,
  DELTA_ERROR_TYPE,
  ERROR_URI_PREFIX
};
