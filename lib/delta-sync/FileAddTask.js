import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import Task from "./Task.js";
import {
  DOWNLOAD_NOT_STARTED_STATUS,
  DOWNLOAD_ONGOING_STATUS,
  DOWNLOAD_FAILURE_STATUS,
  DOWNLOAD_SUCCESS_STATUS,
  REMAPPING_NOT_STARTED_STATUS,
  REMAPPING_ONGOING_STATUS,
  REMAPPING_FAILURE_STATUS,
  REMAPPING_SUCCESS_STATUS,
  MOVING_NOT_STARTED_STATUS,
  MOVING_ONGOING_STATUS,
  MOVING_FAILURE_STATUS,
  MOVING_SUCCESS_STATUS,
  MAX_DOWNLOAD_ATTEMPTS,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  JOB_CREATOR_URI,
  JOBS_GRAPH,
  PREFIXES
} from "../../config.js";

export default class FileAddTask extends Task {
  constructor(subjectID, args = {}) {
    super(subjectID, args);
    this._downloadAttempts = args.downloadAttempts || 0;
    this._downloadStatus = args.downloadStatus || DOWNLOAD_NOT_STARTED_STATUS;
    this._remappingStatus = args.remappingStatus || REMAPPING_NOT_STARTED_STATUS;
    this._movingStatus = args.movingStatus || MOVING_NOT_STARTED_STATUS;
    this._maxDownloadAttempts = MAX_DOWNLOAD_ATTEMPTS;
  }

  get downloadAttempts() {
    return this._downloadAttempts;
  }
  get maxDownloadAttempts() {
    return this._maxDownloadAttempts;
  }

  async madeDownloadAttempt() {
    //Increases the download attempts
    this._downloadAttempts++;
    
    //Remove old number and put new number in the database
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadAttempts ?attempts .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadAttempts ${mu.sparqlEscapeInt(this.downloadAttempts)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadAttempts ?attempts
        }
      }
    `;
    await updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
    
    //If threshold reached, set failure and abort
    if (this._downloadAttempts >= this._maxDownloadAttempts) {
      await this.setDownloadFailure();
    }
    return;
  }

  async setDownloadStatus(status) {
    this._downloadStatus = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadStatus ?dlstatus .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadStatus ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:downloadStatus ?dlstatus .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
  get downloadStatus() {
    return this._downloadStatus;
  }
  async setDownloadSuccess() {
    this.setDownloadStatus(DOWNLOAD_SUCCESS_STATUS);
  }
  async setDownloadFailure() {
    this.setDownloadStatus(DOWNLOAD_FAILURE_STATUS);
  }
  async setDownloadOngoing() {
    this.setDownloadStatus(DOWNLOAD_ONGOING_STATUS);
  }

  async setRemappingStatus(status) {
    this._remappingStatus = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:remappingStatus ?dlstatus .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:remappingStatus ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:remappingStatus ?dlstatus .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
  get remappingStatus() {
    return this._remappingStatus;
  }
  async setRemappingOngoing() {
    this.setRemappingStatus(REMAPPING_ONGOING_STATUS);
  }
  async setRemappingSuccess() {
    this.setRemappingStatus(REMAPPING_SUCCESS_STATUS);
  }
  async setRemappingFailure() {
    this.setRemappingStatus(REMAPPING_FAILURE_STATUS);
  }

  async setMovingStatus(status) {
    this._movingStatus = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:movingStatus ?dlstatus .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:movingStatus ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:movingStatus ?dlstatus .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
  get movingStatus() {
    return this._movingStatus;
  }
  async setMovingOngoing() {
    this.setMovingStatus(MOVING_ONGOING_STATUS);
  }
  async setMovingSuccess() {
    this.setMovingStatus(MOVING_SUCCESS_STATUS);
  }
  async setMovingFailure() {
    this.setMovingStatus(MOVING_FAILURE_STATUS);
  }

  async persist() {
    let queryString = `
      ${PREFIXES}
      
      INSERT DATA {
        GRAPH <${JOBS_GRAPH}> {
          
          ${mu.sparqlEscapeUri(this.uri)}
            a ext:SyncTask ;
            a ext:FileAddTask ;
            mu:uuid ${mu.sparqlEscapeString(this.muuuid)} ;
            ext:subject ${mu.sparqlEscapeString(this.subject)} ;
            adms:status ${mu.sparqlEscapeUri(this.status)} ;
            ext:downloadAttempts ${mu.sparqlEscapeInt(this.downloadAttempts)} ;
            ext:downloadStatus ${mu.sparqlEscapeUri(this.downloadStatus)} ;
            ext:remappingStatus ${mu.sparqlEscapeUri(this.remappingStatus)} ;
            ext:movingStatus ${mu.sparqlEscapeUri(this.movingStatus)} ;
            dct:creator ${mu.sparqlEscapeUri(JOB_CREATOR_URI)} ;
            dct:created ${mu.sparqlEscapeDateTime(this.created)} ;
            dct:modified ${mu.sparqlEscapeDateTime(this.modified)} ;
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
}

