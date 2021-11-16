import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import Task from "./Task.js";
import {
  UPDATE_NOT_STARTED_STATUS,
  UPDATE_ONGOING_STATUS,
  UPDATE_FAILURE_STATUS,
  UPDATE_SUCCESS_STATUS,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  JOB_CREATOR_URI,
  JOBS_GRAPH,
  PREFIXES
} from "../../config.js";

export default class FileUpdateTask extends Task {
  constructor(subjectID, args = {}) {
    super(subjectID, args);
    this._updateStatus = args.updateStatus || UPDATE_NOT_STARTED_STATUS;
  }

  get updateStatus() {
    return this._updateStatus;
  }

  async setUpdateStatus(status) {
    this._updateStatus = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:updateStatus ?dlstatus .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:updateStatus ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:updateStatus ?dlstatus .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
  async setUpdateOngoing() {
    this.setUpdateStatus(UPDATE_ONGOING_STATUS);
  }
  async setUpdateSuccess() {
    this.setUpdateStatus(UPDATE_SUCCESS_STATUS);
  }
  async setUpdateFailure() {
    this.setUpdateStatus(UPDATE_FAILURE_STATUS);
  }

  async persist() {
    let queryString = `
      ${PREFIXES}
      
      INSERT DATA {
        GRAPH <${JOBS_GRAPH}> {
          
          ${mu.sparqlEscapeUri(this.uri)}
            a ext:SyncTask ;
            a ext:FileUpdateTask ;
            mu:uuid ${mu.sparqlEscapeString(this.muuuid)} ;
            ext:subject ${mu.sparqlEscapeUri(this.subject)} ;
            adms:status ${mu.sparqlEscapeUri(this.status)} ;
            ext:updateStatus ${mu.sparqlEscapeUri(this.updateStatus)} ;
            dct:creator ${mu.sparqlEscapeUri(JOB_CREATOR_URI)} ;
            dct:created ${mu.sparqlEscapeDateTime(this.created)} ;
            dct:modified ${mu.sparqlEscapeDateTime(this.modified)} ;
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
}

