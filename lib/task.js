import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { PREFIXES, STATUS_SCHEDULED, TASK_TYPE, TASK_URI_PREFIX } from './constants';
import { parseResult } from './utils';

export async function createTask( graph,
                                  job,
                                  index,
                                  operation,
                                  status = STATUS_SCHEDULED,
                                  parentTasks = [],
                                  inputContainers = [] ){
  const id = uuid();
  const uri = TASK_URI_PREFIX + id;
  const created = new Date();

  let parentTaskTriples = '';

  if(parentTasks.length){
    parentTaskTriples = parentTasks
      .map(parent => `${sparqlEscapeUri(uri)} cogs:dependsOn ${sparqlEscapeUri(parent)}.`)
      .join('\n');
  }

  let inputContainerTriples = '';

  if(inputContainers.length){
    inputContainerTriples = inputContainers
          .map(container => `${sparqlEscapeUri(uri)} task:inputContainer ${sparqlEscapeUri(container)}.`)
        .join('\n');
  }

  const insertQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(graph)} {
       ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(TASK_TYPE)};
                mu:uuid ${sparqlEscapeString(id)};
                dct:isPartOf ${sparqlEscapeUri(job)};
                dct:created ${sparqlEscapeDateTime(created)};
                dct:modified ${sparqlEscapeDateTime(created)};
                adms:status ${sparqlEscapeUri(status)};
                task:index ${sparqlEscapeString(index)};
                task:operation ${sparqlEscapeUri(operation)}.
        ${parentTaskTriples}
        ${inputContainerTriples}
      }
    }
  `;

  await update(insertQuery);

  return uri;
}
