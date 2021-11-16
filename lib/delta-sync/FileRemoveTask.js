import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import Task from "./Task.js";
import {
  REMOVE_NOT_STARTED_STATUS,
  REMOVE_ONGOING_STATUS,
  REMOVE_FAILURE_STATUS,
  REMOVE_SUCCESS_STATUS,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  JOB_CREATOR_URI,
  JOBS_GRAPH,
  PREFIXES
} from "../../config.js";

export default class FileRemoveTask extends Task {
  constructor(subjectID, args = {}) {
    super(subjectID, args);
    this._removeStatus = args.removeStatus || REMOVE_NOT_STARTED_STATUS;
  }

  get removeStatus() {
    return this._removeStatus;
  }

  async setRemoveStatus(status) {
    this._removeStatus = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:removeStatus ?dlstatus .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:removeStatus ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} ext:removeStatus ?dlstatus .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
  async setRemoveOngoing() {
    this.setRemoveStatus(REMOVE_ONGOING_STATUS);
  }
  async setRemoveSuccess() {
    this.setRemoveStatus(REMOVE_SUCCESS_STATUS);
  }
  async setRemoveFailure() {
    this.setRemoveStatus(REMOVE_FAILURE_STATUS);
  }

  async persist() {
    const queryString = `
      ${PREFIXES}
      
      INSERT DATA {
        GRAPH <${JOBS_GRAPH}> {
          
          ${mu.sparqlEscapeUri(this.uri)}
            a ext:SyncTask ;
            a ext:FileRemoveTask ;
            mu:uuid ${mu.sparqlEscapeString(this.muuuid)} ;
            ext:subject ${mu.sparqlEscapeString(this.subject)} ;
            adms:status ${mu.sparqlEscapeUri(this.status)} ;
            ext:removeStatus ${mu.sparqlEscapeUri(this.removeStatus)} ;
            dct:creator ${mu.sparqlEscapeUri(JOB_CREATOR_URI)} ;
            dct:created ${mu.sparqlEscapeDateTime(this.created)} ;
            dct:modified ${mu.sparqlEscapeDateTime(this.modified)} ;
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
}

