import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import {
  TASK_NOT_STARTED_STATUS,
  TASK_ONGOING_STATUS,
  TASK_FAILED_STATUS,
  TASK_SUCCESS_STATUS,
  TASK_URI_PREFIX,
  JOBS_GRAPH,
  PREFIXES,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  JOB_CREATOR_URI
} from "../../config.js";

export default class Task {

  //JavaScript does not allow sync constructor, so always use:
  //    let t = new Task();
  //    await t.persistTask();
  constructor(subjectID, args) {
    let { muuid, status, created, modified, subject, errors } = args || {};
    this._muuuid = muuid || mu.uuid();
    this._status = status || TASK_NOT_STARTED_STATUS;
    this._created = created || new Date();
    this._modified = modified || new Date();
    this._subject = subject || subjectID; //id of the subject 
    this._errors = errors || []; //Collection of id's of errors
  }

  async setStatus(status) {
    this._status = status;
    const queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
      INSERT {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} adms:status ${mu.sparqlEscapeUri(status)} .
        }
      }
      WHERE {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          ${mu.sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }

  get muuuid() {
    return this._muuuid;
  }
  get uri() {
    return TASK_URI_PREFIX.concat(this.muuuid);
  }
  get status() {
    return this._status;
  }
  get created() {
    return this._created;
  }
  get modified() {
    return this._modified;
  }
  get subject() {
    return this._subject;
  }
  get errors() {
    return this._errors;
  }

  async addError(errorID) {
    //Store a triple to add this error to this task
    //TODO implement, but not sure how exactly yet. What is an error? A uuid referencing what precisely?
  }

  async persistTask() {
    const queryString = `
      ${PREFIXES}
      
      INSERT DATA {
        GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
          
          ${mu.sparqlEscapeUri(uri)}
            a ext:SyncTask ;
            mu:uuid ${mu.sparqlEscapeString(this.muuuid)} ;
            ext:subject ${mu.sparqlEscapeString(this.subject)} ;
            adms:status ${mu.sparqlEscapeUri(this.status)} ;
            dct:creator ${mu.sparqlEscapeUri(JOB_CREATOR_URI)} ;
            dct:created ${mu.sparqlEscapeDateTime(this.created)} ;
            dct:modified ${mu.sparqlEscapeDateTime(this.modified)} ;
        }
      }
    `;
    return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
  }
}

