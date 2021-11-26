import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { DELTA_SYNC_TASK_OPERATION, PREFIXES } from './constants';
import { createTask } from './task';

export async function createDeltaSyncTask(graph, job, index, status, deltaFile, parentTask) {
  const task = await createTask( graph,
                                 job,
                                 index,
                                 DELTA_SYNC_TASK_OPERATION,
                                 status,
                                 parentTask ? [ parentTask ] : []
                               );


  const id = uuid();
  const containerUri = `http://data.lblod.info/id/dataContainers/${id}`;

  //TODO: the jobs model feels a bit heavy for this,
  // but still the value is that the dashboard makes it nice to follow
  const addDeltaTimeStampQuery = `
   ${PREFIXES}

   INSERT DATA {
     GRAPH ${sparqlEscapeUri(graph)} {
      ${sparqlEscapeUri(containerUri)} a nfo:DataContainer.
      ${sparqlEscapeUri(containerUri)} dct:subject <http://redpencil.data.gift/id/concept/DeltaSync/DeltafileInfo>.
      ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(id)}.
      ${sparqlEscapeUri(containerUri)} ext:hasDeltafileTimestamp ${sparqlEscapeDateTime(deltaFile.created)}.
      ${sparqlEscapeUri(containerUri)} ext:hasDeltafileId ${sparqlEscapeString(deltaFile.id)}.
      ${sparqlEscapeUri(containerUri)} ext:hasDeltafileName ${sparqlEscapeString(deltaFile.name)}.
      ${sparqlEscapeUri(task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
      ${sparqlEscapeUri(task)} task:inputContainer ${sparqlEscapeUri(containerUri)}.
     }
    }
  `;

  await update(addDeltaTimeStampQuery);

  return task;
}
