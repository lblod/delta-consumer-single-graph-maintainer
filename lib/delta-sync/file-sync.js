import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { escapeRDFTerm, storeError, tripleArrayToMeta, bodyArrayToMeta, tripleArrayToNewMeta } from "../utils.js";
import FileAddTask from "./FileAddTask.js";
import FileRemoveTask from "./FileRemoveTask.js";
import FileUpdateTask from "./FileUpdateTask.js";
import fs from "fs";
import path from "path";
import url from "url";
import fetch from 'node-fetch';
import {
  DISABLE_FILE_INGEST,
  PREFIXES,
  TEMP_FILE_GRAPH,
  SYNC_BASE_URL,
  DOWNLOAD_FILE_PATH,
  FILE_FOLDER,
  INGEST_GRAPH,
  TEMP_FILE_REMOVAL_GRAPH,
  JOBS_GRAPH,
  REMAPPING,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  DOWNLOAD_FILE_ENDPOINT,
  TASK_NOT_STARTED_STATUS, TASK_ONGOING_STATUS, TASK_FAILED_STATUS, TASK_SUCCESS_STATUS,
  DOWNLOAD_NOT_STARTED_STATUS, DOWNLOAD_ONGOING_STATUS, DOWNLOAD_FAILURE_STATUS, DOWNLOAD_SUCCESS_STATUS,
  REMAPPING_NOT_STARTED_STATUS, REMAPPING_ONGOING_STATUS, REMAPPING_FAILURE_STATUS, REMAPPING_SUCCESS_STATUS,
  MOVING_NOT_STARTED_STATUS, MOVING_ONGOING_STATUS, MOVING_FAILURE_STATUS, MOVING_SUCCESS_STATUS,
  REMOVE_NOT_STARTED_STATUS, REMOVE_ONGOING_STATUS, REMOVE_FAILURE_STATUS, REMOVE_SUCCESS_STATUS,
  UPDATE_NOT_STARTED_STATUS, UPDATE_ONGOING_STATUS, UPDATE_FAILURE_STATUS, UPDATE_SUCCESS_STATUS
} from '../../config.js';

/**
 * Start the synchronisation of files.
 *
 * @public
 * @async
 * @function startFileSync
 * @return {undefined} - No useful return value, side effects only
 */
export async function startFileSync() {
  console.info(`DISABLE_FILE_INGEST: ${DISABLE_FILE_INGEST}`);
  try {
    if (!DISABLE_FILE_INGEST) {
      await runFileSync(); 
      console.log("File sync finished");
    }
    else {
      console.warn('Automated file ingest disabled');
    }
  }
  catch (err) {
    //Store the error to the database, also include the stacktrace for better debugging (even though the stack trace might be useless when the code changes over time).
    console.error(err);
    await storeError(err.toString());
  }
}

/**
 * Run file synchronisation. Starts the process of scheduling different kinds of tasks and executes the one after the other.
 *
 * @public
 * @async
 * @function runFileSync
 * @return {undefined} - No useful return value, side effects only
 */
async function runFileSync() {
  //Collect tasks that are ongoing
  const ongoingFileAddTasks = await getOngoingFileAddTasks();
  //Create tasks for new virtual files, excluding the already ongoing tasks
  const fileAddTasks = await scheduleNewFileAddTasks(ongoingFileAddTasks);
  //Loop over all the tasks and start the processes
  for (let addTasks of [ ongoingFileAddTasks, fileAddTasks ])
    for (let task of addTasks) {
      try {
        await startFileAddTask(task);
        await downloadFileDuringTask(task);
        await remapFileMetadata(task);
        await moveFileMetadata(task);
        await finishFileAddTask(task);
      }
      catch (err) {
        console.error(err);
        await storeError(err.toString());
        //Set tasks to failure only if approprate
        await finishFileAddTask(task);
      }
    }

  //Get tasks about full metadata indicating removal of files
  const fileRemoveTasks = await scheduleNewFileRemoveTasks();
  for (let task of fileRemoveTasks) {
    try {
      await startFileRemoveTask(task);
      await removeFileDuringTask(task);
      await finishFileRemoveTask(task);
    }
    catch (err) {
      console.error(err);
      await storeError(err.toString());
      //Same as before, only finish a task that can be finished
      await finishFileRemoveTask(task);
    }
  }

  //Schedule tasks about a group of metadata about individuals that already exist in the ingest graph
  const fileUpdateTasks = await scheduleNewFileUpdateTasks();
  for (let task of fileUpdateTasks) {
    try {
      await startFileUpdateTask(task);
      await updateFileDuringTask(task);
      await finishFileUpdateTask(task);
    }
    catch (err) {
      console.error(err);
      await storeError(err.toString());
      await finishFileUpdateTask(task);
    }
  }
}

/*******************************************************************************
 * Subtask functions
*******************************************************************************/

/*
 * Starts a FileAddTask. It sets the status to ongoing.
 *
 * @private
 * @async
 * @function startFileAddTask
 * @param {FileAddTask} task - Task to start
 * @returns {void} Nothing
 */
async function startFileAddTask(task) {
  switch (task.status) {
    case TASK_NOT_STARTED_STATUS:
      await task.setStatus(TASK_ONGOING_STATUS);
      break;
    case TASK_ONGOING_STATUS:
      break;
    default:
      throw Error(`Task ${task.muuuid} cannot be started because it has already finished (success or failure) with status: ${task.status}`);
      break;
  }
}

/*
 * This changes the download status of the task to ongoing and starts the download process. On failure, it increments the download attempts. On permanent failure, it sets the status to failure and the rest of the processing is cancelled.
 *
 * @private
 * @async
 * @function downloadFileDuringTask
 * @param {FileAddTask} task - Task to download the file of
 * @returns {void} Nothing
 */
async function downloadFileDuringTask(task) {
  switch (task.downloadStatus) {
    case DOWNLOAD_NOT_STARTED_STATUS:
      await task.setDownloadOngoing();
      //Some recursion because we don't want overly duplicated code
      return downloadFileDuringTask(task);
    case DOWNLOAD_ONGOING_STATUS:
      //Perform a(nother) download attempt
      try {
        await downloadFileForTask(task);
      }
      catch (err) {
        //On error, indicate that an attempts has been made
        await task.madeDownloadAttempt();
        //and set the download as permanently failed if no more tries can be made
        if (task.reachedDownloadThreshold)
          task.setDownloadFailure();
        throw err;
      }
      await task.madeDownloadAttempt();
      await task.setDownloadSuccess();
      break;
    case DOWNLOAD_FAILURE_STATUS:
    case DOWNLOAD_SUCCESS_STATUS:
      //Don't do anything, this step is then skipped
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown download status of ${task.downloadStatus}`);
  }
}

/*
 * Sets the remapping status to ongoing and starts the remapping process. Sets the status to success or failure on finish.
 * The remapping process changes the URI based on a transformation table supplied by the configuration and moves physical files in their final location.
 *
 * @private
 * @async
 * @function remapFileMetadata
 * @param {FileAddTask} task - Task for the file to remap
 * @returns {void} Nothing
 */
async function remapFileMetadata(task) {
  //Prevent remapping from happening if the download hasn't succeeded
  if (task.downloadStatus != DOWNLOAD_SUCCESS_STATUS)
    return;

  switch (task.remappingStatus) {
    case REMAPPING_NOT_STARTED_STATUS:
      await task.setRemappingOngoing();
      return remapFileMetadata(task);
    case REMAPPING_ONGOING_STATUS:
      try {
        const succeeded = await remapFileForTask(task);
        //Remapping cannot happen if not all the metadata has arrived, so only set success if the remapping actually secceeded
        if (succeeded)
          await task.setRemappingSuccess();
      }
      catch (err) {
        await task.setRemappingFailure();
        throw err;
      }
      break;
    case REMAPPING_FAILURE_STATUS:
    case REMAPPING_SUCCESS_STATUS:
      //Don't do anything
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown remapping status of ${task.remappingStatus}`);
  }
}

/*
 * Sets the moving status to ongoing and starts the moving process. Sets the status to success or failure on finish.
 * Moving copies the metadata to the ingest graph and removes it from the temp graph.
 *
 * @private
 * @async
 * @function moveFileMetadata
 * @param {FileAddTask} task - Task for the file to remap
 * @returns {void} Nothing
 */
async function moveFileMetadata(task) {
  //Prevent the moving from happening if the download or the remapping wasn't successfull
  if (task.downloadStatus != DOWNLOAD_SUCCESS_STATUS || task.remappingStatus != REMAPPING_SUCCESS_STATUS)
    return;

  switch (task.movingStatus) {
    case MOVING_NOT_STARTED_STATUS:
      await task.setMovingOngoing();
      return moveFileMetadata(task);
    case MOVING_ONGOING_STATUS:
      try {
        const succeeded = await moveFileMetadataForTask(task);
        //Again, data can only be moved if all metadata is present. (Although, if ended up here after the remapping, all the data should be present.)
        if (succeeded)
          await task.setMovingSuccess();
      }
      catch (err) {
        await task.setMovingFailure();
        throw err;
      }
    case MOVING_FAILURE_STATUS:
    case MOVING_SUCCESS_STATUS:
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown moving status of ${task.movingStatus}`);
  }
}

/*
 * Stops a FileAddTask. It sets the status to failure or success based on statusses of the internal processes.
 *
 * @private
 * @async
 * @function finishFileAddTask
 * @param {FileAddTask} task - Task to finish
 * @returns {void} Nothing
 */
async function finishFileAddTask(task) {
  if (task.downloadStatus === DOWNLOAD_SUCCESS_STATUS && task.remappingStatus === REMAPPING_SUCCESS_STATUS && task.movingStatus === MOVING_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  else if (task.downloadStatus === DOWNLOAD_FAILURE_STATUS || task.remappingStatus === REMAPPING_FAILURE_STATUS || task.movingStatus === MOVING_FAILURE_STATUS) {
    await task.setStatus(TASK_FAILED_STATUS);
    //Make sure that next task schedulers do not pick up this metadata again
    await setVUUIDToTaskFailure(task.subject);
  }
}

/*
 * Starts a FileRemoveTask. It sets the status to ongoing.
 *
 * @private
 * @async
 * @function startFileRemoveTask
 * @param {FileRemoveTask} task - Task to start
 * @returns {void} Nothing
 */
async function startFileRemoveTask(task) {
  switch (task.status) {
    case TASK_NOT_STARTED_STATUS:
      await task.setStatus(TASK_ONGOING_STATUS);
      break;
    case TASK_ONGOING_STATUS:
      break;
    default:
      throw Error(`Task ${task.muuuid} cannot be started because it has already finished (succes or failure) with status: ${task.status}`);
      break;
  }
}

/*
 * Sets the status to onoing and tries to remove a file only when all the metadata has arrived. If not the process is suspended until the next run.
 *
 * @private
 * @async
 * @function removeFileDuringTask
 * @param {FileRemoveTask} task - Task for the file to remove
 * @returns {void} Nothing
 */
async function removeFileDuringTask(task) {
  switch (task.removeStatus) {
    case REMOVE_NOT_STARTED_STATUS:
      await task.setRemoveOngoing();
      return removeFileDuringTask(task);
    case REMOVE_ONGOING_STATUS:
      try {
        const succeeded = await removeFileForTask(task);
        //Even though tasks can only be scheduled when all the metadata exists, that check is performed here too, so only set to success if succeeded
        if (succeeded)
          await task.setRemoveSuccess();
      }
      catch (err) {
        await task.setRemoveFailure();
        throw err;
      }
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown remove status of ${task.removeStatus}`);
  }
}

/*
 * Stops a FileRemoveTask. It sets the status to failure or success based on statusses of the internal processes.
 *
 * @private
 * @async
 * @function finishFileRemoveTask
 * @param {FileAddTask} task - Task to finish
 * @returns {void} Nothing
 */
async function finishFileRemoveTask(task) {
  if (task.removeStatus === REMOVE_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  else if (task.removeStatus === REMOVE_FAILURE_STATUS) {
    await task.setStatus(TASK_FAILED_STATUS);
    //Make sure that next task schedulers do not pick up this metadata again
    await setVUUIDToTaskFailure(task.subject, TEMP_FILE_REMOVAL_GRAPH);
  }
}

/*
 * Starts a FileUpdateTask. It sets the status to ongoing.
 *
 * @private
 * @async
 * @function startFileUpdateTask
 * @param {FileUpdateTask} task - Task to start
 * @returns {void} Nothing
 */
async function startFileUpdateTask(task) {
  switch (task.status) {
    case TASK_NOT_STARTED_STATUS:
      await task.setStatus(TASK_ONGOING_STATUS);
      break;
    case TASK_ONGOING_STATUS:
      break;
    default:
      throw Error(`Task ${task.muuuid} cannot be started because it has already finished (succes or failure) with status: ${task.status}`);
      break;
  }
}

/*
 * Sets the status to ongoing and updates the metadata of a file.
 *
 * @private
 * @async
 * @function updateFileDuringTask
 * @param {FileUpdateTask} task - Task for a number of triples that represent updates to metadata of a file
 * @returns {void} Nothing
 */
async function updateFileDuringTask(task) {
  switch (task.updateStatus) {
    case UPDATE_NOT_STARTED_STATUS:
      await task.setUpdateOngoing();
      return updateFileDuringTask(task);
    case UPDATE_ONGOING_STATUS:
      try {
        await updateFileForTask(task);
        await task.setUpdateSuccess();
      }
      catch (err) {
        await task.setUpdateFailure();
        throw err;
      }
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown update status of ${task.updateStatus}`);
  }
}

/*
 * Stops a FileUpdateTask. It sets the status to failure or success based on statusses of the internal processes.
 *
 * @private
 * @async
 * @function finishFileUpdateTask
 * @param {FileUpdateTask} task - Task to finish
 * @returns {void} Nothing
 */
async function finishFileUpdateTask(task) {
  if (task.updateStatus === UPDATE_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  else if (task.updateStatus === UPDATE_FAILURE_STATUS) {
    await task.setStatus(TASK_FAILED_STATUS);
    //Make sure that next task schedulers do not pick up this metadata again
    await setURIToTaskFailure(task.subject);
  }
}

/*******************************************************************************
 * Scheduling tasks
*******************************************************************************/

/*
 * Get FileAddTasks from the database that are still ongoing. These need to be resumed.
 *
 * @private
 * @async
 * @function getOngoingFileAddTasks
 * @returns {Array} List of FileAddTasks that need to be resumed
 */
async function getOngoingFileAddTasks() {
  let tasks = [];

  const queryString = `
    ${PREFIXES}

    SELECT * {
      GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
        ?uri
          rdf:type ext:SyncTask ;
          rdf:type ext:FileAddTask ;
          mu:uuid ?muuuid ;
          adms:status ?status ;
          ext:subject ?subject ;
          ext:downloadAttempts ?downloadAttempts ;
          ext:downloadStatus ?downloadStatus ;
          ext:remappingStatus ?remappingStatus ;
          ext:movingStatus ?movingStatus ;
          dct:creator ?creator ;
          dct:created ?created ;
          dct:modified ?modified .
        { ?uri adms:status ${mu.sparqlEscapeUri(TASK_ONGOING_STATUS)} . }
        UNION
        { ?uri adms:status ${mu.sparqlEscapeUri(TASK_NOT_STARTED_STATUS)} . }
      }
    }
  `;
  const results = await querySudo(queryString);
  for (let binding of results.results.bindings) {
    //Make a new task for every binding and provide all data through it's options object
    tasks.push(new FileAddTask(binding.subject.value, {
      muuid: binding.muuuid.value,
      status: binding.status.value,
      downloadAttempts: Number(binding.downloadAttempts.value),
      downloadStatus: binding.downloadStatus.value,
      remappingStatus: binding.remappingStatus.value,
      movingStatus: binding.movingStatus.value,
      created: new Date(binding.created.value),
      modified: new Date(binding.modified.value)
    }));
  }
  return tasks;
}

/*
 * Schedules new FileAddTasks in the database and also returns them. The supplied ongoing tasks are not double scheduled.
 * New tasks are scheduled when their virtual file UUID is present. That is when the download process can start.
 *
 * @private
 * @async
 * @function scheduleNewFileAddTasks
 * @param {Array} alreadyOngoingTasks - Array of tasks that are already ongoing, from the getOngoingFileAddTasks function
 * @returns {Array} All the tasks that are scheduled and already ongoing
 */
async function scheduleNewFileAddTasks(alreadyOngoingTasks) {
  const queryString = `
    ${PREFIXES}

    SELECT ?vuuid
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([`?vuri`, `mu:uuid`, `?vuuid`])}
        ${tripleArrayToMeta([`?puri`, `nie:dataSource`, `?vuri`])}
        FILTER NOT EXISTS { ${tripleArrayToMeta([`?vuri`, `rdf:type`, `ext:TaskFailure`])} }
      }
    }
  `;
  //Only get the vuuids in an array
  const vuuids = (await querySudo(queryString)).results.bindings.map(b => b.vuuid.value);
  let newTask;
  let newTasks = [];
  for (let vuuid of vuuids) {
    //Only create new task if the vuuid is not already used for an ongoing task
    if (!alreadyOngoingTasks.some(t => t.subject == vuuid)) {
      newTask = new FileAddTask(vuuid);
      await newTask.persist();
      newTasks.push(newTask);
    }
  }
  console.log("Tasks scheduled");
  return newTasks;
}

/*
 * Schedules new FileRemoveTasks in the database and also returns them.
 * A new task is only scheduled when all metadata in the removal graph is present to remove a file.
 *
 * @private
 * @async
 * @function scheduleNewFileRemoveTasks
 * @returns {Array} All the tasks that are scheduled
 */
async function scheduleNewFileRemoveTasks() {
  const body = bodyArrayToMeta([
    [`?vuri`, `rdf:type`, `nfo:FileDataObject`],
    [`?vuri`, `mu:uuid`, `?vuuid`],
    [`?vuri`, `nfo:fileName`, `?filename`],
    [`?vuri`, `dct:format`, `?format`],
    [`?vuri`, `nfo:fileSize`, `?filesize`],
    [`?vuri`, `dbpedia:fileExtension`, `?extension`],
    [`?vuri`, `dct:created`, `?created`],
    [`?vuri`, `dct:modified`, `?modified`],
    [`?puri`, `rdf:type`, `nfo:FileDataObject`],
    [`?puri`, `mu:uuid`, `?puuid`],
    [`?puri`, `nie:dataSource`, `?vuri`],
    [`?puri`, `nfo:fileName`, `?pfilename`],
    [`?puri`, `dct:format`, `?pformat`],
    [`?puri`, `nfo:fileSize`, `?pfilesize`],
    [`?puri`, `dbpedia:fileExtension`, `?pextension`],
    [`?puri`, `dct:created`, `?pcreated`],
    [`?puri`, `dct:modified`, `?pmodified`]
  ]);
  const queryString = `
    ${PREFIXES}
    SELECT ?vuuid
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ${body}
        FILTER NOT EXISTS { ${tripleArrayToMeta([`?vuri`, `rdf:type`, `ext:TaskFailure`])} }
      }
    }
  `;
  const vuuids = (await querySudo(queryString)).results.bindings.map(b => b.vuuid.value);
  let newTask;
  let newTasks = [];
  for (let vuuid of vuuids) {
    newTask = new FileRemoveTask(vuuid);
    await newTask.persist();
    newTasks.push(newTask);
  }
  return newTasks;
}

/*
 * Schedules new FileUpdateTasks in the database and also returns them.
 * A new task is only scheduled when at least one triple in the temp graph is present that is about a URI that also exists in the ingest graph.
 * The tasks subject is now the URI instead of the UUID of the virtual file.
 *
 * @private
 * @async
 * @function scheduleNewFileUpdateTasks
 * @returns {Array} All the tasks that are scheduled
 */
async function scheduleNewFileUpdateTasks() {
  //Split up into two queries because this is less complex
  const queryStringNoReplacements = `
    ${PREFIXES}
    SELECT ?uri {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([`?uri`, `?pred`, `?newval`])}
        FILTER NOT EXISTS { ${tripleArrayToMeta([`?uri`, `rdf:type`, `ext:TaskFailure`])} }
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?uri ?pred ?oldval .
      }
    }
  `;
  const queryStringReplacements = `
    ${PREFIXES}
    SELECT ?uri {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([`?uri`, `?pred`, `?newval`])}
        FILTER NOT EXISTS { ${tripleArrayToMeta([`?uri`, `rdf:type`, `ext:TaskFailure`])} }
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?newuri ?pred ?oldval .
        ?newuri dct:replaces ?uri .
      }
    }
  `;
  const resultsNoReplacements = await querySudo(queryStringNoReplacements);
  const resultsReplacements = await querySudo(queryStringReplacements);
  let tasks = [];
  let newTask;
  for (let results of [ resultsNoReplacements, resultsReplacements ]) {
    for (let binding of results.results.bindings) {
      newTask = new FileUpdateTask(binding.uri.value);
      newTask.persist();
      tasks.push(newTask);
    }
  }
  return tasks;
}

/*******************************************************************************
 * General database access
*******************************************************************************/

/*
 * Get the physical file URI from the given virtual file UUID in the optional specified graph.
 *
 * @private
 * @async
 * @function getPURIFromVUUID
 * @param {String} vuuid - Virtual file UUID that you want the URI from
 * @param {String} graph - Optional. Graph URI to look into, defaults to temp graph
 * @returns {String} URI of the physical file metadata
 */
async function getPURIFromVUUID(vuuid, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const queryString = `
    ${PREFIXES}
    SELECT ?puri {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ${bodyArrayToMeta([
          [`?vuri`, `mu:uuid`, mu.sparqlEscapeString(vuuid)],
          [`?puri`, `nie:dataSource`, `?vuri`]
        ])}
      }
    }
  `;
  const response = await querySudo(queryString);
  //Carefully get the first result (only one should be there)
  if (response.results.bindings.length > 0) {
    return response.results.bindings[0].puri.value;
  }
}

/**
 * Change all triples about the old physical file URI to the new URI. It also inserts a triple to indicate replacement.
 *
 * @async
 * @private
 * @function updatePURI
 * @return {void} Nothing
 */
async function updatePURI(olduri, newuri) {
  const queryString = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(olduri), `?p`, `?o`], "UPDATE1")}
      }
    }
    INSERT {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(newuri), `?p`, `?o`], "UPDATE1")}
        ${tripleArrayToNewMeta([mu.sparqlEscapeUri(newuri), `dct:replaces`, mu.sparqlEscapeUri(olduri)])}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(olduri), `?p`, `?o`], "UPDATE1")}
      }
    }
  `;
  return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

/*
 * Checks if all metadata for the file with a given virtual file UUID is present.
 *
 * @private
 * @async
 * @function isMetadataFull
 * @param {String} vuuid - Virtual file UUID that you want to check
 * @param {String} graph - Optional. Graph URI to look into, defaults to temp graph
 * @returns {Boolean} Whether or not all metadata is present
 */
async function isMetadataFull(vuuid, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const body = bodyArrayToMeta([
          [`?vuri`, `rdf:type`, `nfo:FileDataObject`],
          [`?vuri`, `mu:uuid`, mu.sparqlEscapeString(vuuid)],
          [`?vuri`, `nfo:fileName`, `?filename`],
          [`?vuri`, `dct:format`, `?format`],
          [`?vuri`, `nfo:fileSize`, `?filesize`],
          [`?vuri`, `dbpedia:fileExtension`, `?extension`],
          [`?vuri`, `dct:created`, `?created`],
          [`?vuri`, `dct:modified`, `?modified`],
          [`?puri`, `rdf:type`, `nfo:FileDataObject`],
          [`?puri`, `mu:uuid`, `?puuid`],
          [`?puri`, `nie:dataSource`, `?vuri`],
          [`?puri`, `nfo:fileName`, `?pfilename`],
          [`?puri`, `dct:format`, `?pformat`],
          [`?puri`, `nfo:fileSize`, `?pfilesize`],
          [`?puri`, `dbpedia:fileExtension`, `?pextension`],
          [`?puri`, `dct:created`, `?pcreated`],
          [`?puri`, `dct:modified`, `?pmodified`]
  ]);
  const queryString = `
    ${PREFIXES}
    SELECT * {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ${body}
        OPTIONAL { ${tripleArrayToMeta([`?puri`, `dct:replaces`, `?oldpuri`])} }
      }
    }
  `;
  const response = await querySudo(queryString);
  //Only return true if at least one binding is present
  return (response.results.bindings.length > 0);
}

/*
 * Sets an individual in the database to a permanent task failure, so that future task schedulers can exclude this metadata.
 * This works on the virtual file UUID.
 *
 * @private
 * @async
 * @function setVUUIDToTaskFailure
 * @param {String} vuuid - Virtual file uuid that needs to be marked as a task failure
 * @param {String} graph - Graph in which to insert data, defaults to the temp graph
 * @returns {void} Nothing
 */
async function setVUUIDToTaskFailure(vuuid, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const queryString = `
    ${PREFIXES}
    INSERT {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ${tripleArrayToNewMeta([`?vuri`, `rdf:type`, `ext:TaskFailure`])}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ${tripleArrayToMeta([`?vuri`, `mu:uuid`, mu.sparqlEscapeString(vuuid)])}
      }
    }
  `;
  return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

/*
 * Sets an individual in the database to a permanent task failure, so that future task schedulers can exclude this metadata.
 * This works on a URI (virtual or physical file).
 *
 * @private
 * @async
 * @function setURIToTaskFailure
 * @param {String} uri - Virtual file uri that needs to be marked as a task failure
 * @param {String} graph - Graph in which to insert data, defaults to the temp graph
 * @returns {void} Nothing
 */
async function setURIToTaskFailure(uri, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const queryString = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ${tripleArrayToNewMeta([mu.sparqlEscapeUri(uri), `rdf:type`, `ext:TaskFailure`])}
      }
    }
  `;
  return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

/*******************************************************************************
 * File download and storage
*******************************************************************************/

/*
 * Takes care of downloading the file for the given task.
 *
 * @private
 * @async
 * @function downloadFileForTask
 * @param {FileAddTask} task - Task for which the file needs to be downloaded
 * @returns {void} Nothing
 */
async function downloadFileForTask(task) {
  //PURI is needed for the file path (this can later change during remapping)
  const puri = await getPURIFromVUUID(task.subject);
  const path = puri.replace("share://", "/share/");
  return downloadAndSaveFile(task.subject, path);
}

/**
 * Download a file from the producer and put it in the given path, on the shared volume
 *
 * @async
 * @private
 * @function downloadAndSaveFile
 * @param {String} vuuid - The UUID of the virtual file to be downloaded
 * @param {String} filepath - The file path, with name included, of the destination of the download
 * @return {void} Nothing
 */
async function downloadAndSaveFile(vuuid, filepath) {

  //If, for some reason, the filepath does not start with the mounted volume's path, error or files get lost.
  if (!filepath.startsWith(FILE_FOLDER)) {
    throw new Error(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}. Files can get lost like this!`);
  }

  //Prepare the URL
  const fileDownloadURL = DOWNLOAD_FILE_ENDPOINT.replace(":id", vuuid);

  try {
    await createTargetDir(filepath);
    const writeStream = await createWriteStream(filepath);
    const response = await fetch(fileDownloadURL);
    return response.body.pipe(writeStream);
  }
  catch (err) {
    //On error, remove the file path again. If left behind, next download attempts are prevented because the file already exists
    await deleteFile(filepath);
    throw err;
  }
}

/*
 * Creates all folders in the given folder or file path recursively if they do not exist
 *
 * @private
 * @async
 * @function creteTargetDir
 * @param {String} filepath - Target folder or file path of which all folders need to be created
 * ®returns {void} Nothing
 */
function createTargetDir(filepath) {
  return new Promise((resolve, reject) => {
    //Get the folderpath, without filename
    const targetdir = path.dirname(filepath);
    //With the recursive property, no error is raised if the folders already exist
    fs.mkdir(targetdir, { recursive: true }, err => {
      if (err) {
        reject(new Error(`Target directory ${targetdir} creation failed`));
      }
      resolve();
    });
  });
}

/*
 * Creates a write stream to the filepath, not overwriting existing files.
 * Closes by itself on error and success. Resolves only on ready state.
 *
 * @private
 * @async
 * @function createWriteStream
 * @param {String} filepath - Path to create the write stream for
 * @returns {WriteStream} The created write stream
 */
function createWriteStream(filepath) {
  return new Promise((resolve, reject) => {
    //With the 'x' flag, error if file already exists
    const writeStream = fs.createWriteStream(filepath, { flags: "wx" });
    writeStream.on("finish", () => {
      writeStream.close();
    });
    writeStream.on("error", async (err) => {
      if (err.errno == -17) {
        console.log("File already exists on local storage, ignoring and not downloading the file again");
        resolve(writeStream);
      } else {
        writeStream.close();
        reject(`Error while downloading and saving file ${filepath} on the consumer, during writing to the filestream.`);
      }
    });
    writeStream.on("ready", async () => {
      resolve(writeStream);
    });
  });
}

/**
 * Rename a file from their old name to a new name.
 *
 * @async
 * @private
 * @deprecated because it assumes a certain path on the filename
 * @function renameFile
 * @param {string} oldname - The old name to be renamed, will be located in the shared volume path.
 * @param {string} newname - The new name for the file.
 * @return {void} Nothing
 */
function renameFile(oldname, newname) {
  return new Promise((resolve, reject) => {
    const oldpath = path.join(FILE_FOLDER, oldname);
    const newpath = path.join(FILE_FOLDER, newname);
    console.log(`Rename ${oldpath} into ${newpath}`);

    fs.rename(oldpath, newpath, (err) => {
      if (err) reject(new StackableError(`Error while renaming file ${oldpath} into ${newpath}.`, err));
      resolve();
    });
  });
}

/**
 * Delete a file. Ignores when the file does not exist.
 *
 * @async
 * @private
 * @function deleteFile
 * @param {string} filepath - Name with full path of the file to be removed.
 * @return {void} Nothing
 */
function deleteFile(filepath) {
  return new Promise((resolve, reject) => {
    fs.rm(filepath, { force: false }, (err) => {
      if (err) {
        //Ignore when file is already removed
        if (err.errno != -2)
          reject(new StackableError(`Error while deleting file ${filepath}.`, err))
        resolve();
      }
      else {
        console.log("Removing file gave message:", err);
        resolve()
      }
    });
  });
}

/**
 * Move a file to a new location and (potential) name.
 *
 * @async
 * @private
 * @function moveFile
 * @param {string} oldfilepath - original path (including name) of the file to be moved
 * @param {string} newfilepath - new path (including name) to move the file to
 * @return {void} Nothing
 */
function moveFile(oldfilepath, newfilepath) {
  return createTargetDir(newfilepath).then(() =>
    new Promise((resolve, reject) => {
      fs.rename(oldfilepath, newfilepath, (err) => {
        if (err) {
          reject(new Error(`Moving the file from ${oldfilepath} to ${newfilepath} failed.`));
        }
        resolve();
      });
    })
  );
}

/*******************************************************************************
 * Remapping
*******************************************************************************/

/*
 * Remaps the metadata and the file location for a given task.
 * Remapping is done with a configuration object that maps old location to new location.
 *
 * @private
 * @async
 * @functoin remapFileForTask
 * @param {FileAddTask} task - Task for the file to remap
 * @returns {Boolean} Indicates if the remapping was possible or not (yet). Does not indicate if it was needed or not. Error on failure.
 */
async function remapFileForTask(task) {
  const isComplete = await isMetadataFull(task.subject);
  if (isComplete) {
    const shareURI = await getPURIFromVUUID(task.subject);
    const newShareURI = transformURI(shareURI);
    //If no remapping is necessary, return immediately.
    if (!newShareURI) return true;

    //Move file to their final destination
    const oldpath = shareURI.replace("share://", "/share/");
    const newpath = newShareURI.replace("share://", "/share/");
    await moveFile(oldpath, newpath);

    //Update all uri's in the database
    await updatePURI(shareURI, newShareURI);

    return true;
  }
  else {
    return false;
  }
}

/**
 * This maps the path part of a given URI to the mapping object to construct a new URI. It returns false when no remapping is needed (because there is no entry in the remapping object in the config.
 *
 * @private
 * @function transformURI
 * @param {String} shareURI - URI of the physical file that needs to be remapped
 * @return {(Boolean|String)} Returns either a string with the new URI or false to indicate that no remapping is necessary.
 */
function transformURI(shareURI) {
  const origpath = shareURI.replace("share://", "/");
  const basename = path.basename(origpath);
  const dirname = path.dirname(origpath);
  //Lookup the path in the config for a new mapping
  const remappedDirname = REMAPPING[dirname];
  //If no new mapping exists, return immediately
  if (!remappedDirname) return false;
  const newpath = path.join("/", remappedDirname, basename);
  const newShareURI = ["share:/", path.normalize(newpath)].join("");
  return newShareURI;
}

/*******************************************************************************
 * Moving
*******************************************************************************/

/*
 * Moves metadata to the ingest graph and removes from the temp graph.
 *
 * @private
 * @async
 * @function moveFileMetadataForTask
 * @param {FileAddTask} task - Task of wich the metadata needs to be moved
 * @returns {Boolean} Boolean to indicate if the move was possible or not (yet). Does not indicate success or failure (true on success, but Error on failure).
 */
async function moveFileMetadataForTask(task) {
  const isComplete = await isMetadataFull(task.subject);
  if (isComplete) {
    await moveFullMetadataToIngest(task.subject);
    await moveFullMetadataDelete(task.subject);
    return true;
  }
  else {
    return false;
  }
}

/*
 * Removes only complete metadata from the temp graph in the database.
 * Sends a mu-call-scope-id to possibly prevent the deltanotifier from propagating.
 *
 * @private
 * @async
 * @function moveFullMetadataDelete
 * @param {String} vuuid - Virtual file UUID of which metadatae needs to be moved
 * @returns {void} Nothing
 */
async function moveFullMetadataDelete(vuuid) {
  const body = bodyArrayToMeta([
    [`?vuri`, `rdf:type`,              `nfo:FileDataObject`,         `?graph`],
    [`?vuri`, `mu:uuid`,               mu.sparqlEscapeString(vuuid), `?graph`],
    [`?vuri`, `nfo:fileName`,          `?filename`,                  `?graph`],
    [`?vuri`, `dct:format`,            `?format`,                    `?graph`],
    [`?vuri`, `nfo:fileSize`,          `?filesize`,                  `?graph`],
    [`?vuri`, `dbpedia:fileExtension`, `?extension`,                 `?graph`],
    [`?vuri`, `dct:created`,           `?created`,                   `?graph`],
    [`?vuri`, `dct:modified`,          `?modified`,                  `?graph`],
    [`?puri`, `rdf:type`,              `nfo:FileDataObject`,         `?graph`],
    [`?puri`, `mu:uuid`,               `?puuid`,                     `?graph`],
    [`?puri`, `nie:dataSource`,        `?vuri`,                      `?graph`],
    [`?puri`, `nfo:fileName`,          `?pfilename`,                 `?graph`],
    [`?puri`, `dct:format`,            `?pformat`,                   `?graph`],
    [`?puri`, `nfo:fileSize`,          `?pfilesize`,                 `?graph`],
    [`?puri`, `dbpedia:fileExtension`, `?pextension`,                `?graph`],
    [`?puri`, `dct:created`,           `?pcreated`,                  `?graph`],
    [`?puri`, `dct:modified`,          `?pmodified`,                 `?graph`]
  ]);
  const queryString = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${body}
        ${tripleArrayToMeta([`?puri`, `dct:replaces`, `?oldpuri`], "MOVE1")}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${body}
        OPTIONAL { ${tripleArrayToMeta([`?puri`, `dct:replaces`, `?oldpuri`], "MOVE1")} }
      }
    }
  `;
  return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

/*
 * Moves only complete metadata from the temp graph in the database to the ingest graph.
 *
 * @private
 * @async
 * @function moveFullMetadataToIngest
 * @param {String} vuuid - Virtual file UUID of which metadatae needs to be moved
 * @returns {void} Nothing
 */
async function moveFullMetadataToIngest(vuuid) {
  const body = bodyArrayToMeta([
    [`?vuri`, `rdf:type`,              `nfo:FileDataObject`,        `?graph`],
    [`?vuri`, `mu:uuid`,               mu.sparqlEscapeString(vuuid) ],
    [`?vuri`, `nfo:fileName`,          `?filename`                  ],
    [`?vuri`, `dct:format`,            `?format`                    ],
    [`?vuri`, `nfo:fileSize`,          `?filesize`                  ],
    [`?vuri`, `dbpedia:fileExtension`, `?extension`                 ],
    [`?vuri`, `dct:created`,           `?created`                   ],
    [`?vuri`, `dct:modified`,          `?modified`                  ],
    [`?puri`, `rdf:type`,              `nfo:FileDataObject`         ],
    [`?puri`, `mu:uuid`,               `?puuid`                     ],
    [`?puri`, `nie:dataSource`,        `?vuri`                      ],
    [`?puri`, `nfo:fileName`,          `?pfilename`                 ],
    [`?puri`, `dct:format`,            `?pformat`                   ],
    [`?puri`, `nfo:fileSize`,          `?pfilesize`                 ],
    [`?puri`, `dbpedia:fileExtension`, `?pextension`                ],
    [`?puri`, `dct:created`,           `?pcreated`                  ],
    [`?puri`, `dct:modified`,          `?pmodified`                 ]
  ]);
  const queryString = `
    ${PREFIXES}
    INSERT {
      GRAPH ?graph {
        ?vuri rdf:type nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri rdf:type nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified ;
          dct:replaces ?oldpuri .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${body}
        OPTIONAL { ${tripleArrayToMeta([`?puri`, `dct:replaces`, `?oldpuri`])} }
        BIND (?graph AS ?graph)
      }
    }
  `;
  //No mu-call-scope-id because this needs to be propagated through the deltanotifier
  return updateSudo(queryString);
}

/*******************************************************************************
 * Removing
*******************************************************************************/

/*
 * Remove the file indicated by the task, only if all metadata is present.
 *
 * @private
 * @async
 * @function removeFileForTask
 * @param {FileRemoveTask} task
 * @returns {Boolean} Boolean to indicate of removal was possible and successfull or not. Error on failure.
 */
async function removeFileForTask(task) {
  const shareURI = await getPURIFromVUUID(task.subject, TEMP_FILE_REMOVAL_GRAPH);
  const isComplete = await isMetadataFull(task.subject, TEMP_FILE_REMOVAL_GRAPH);
  if (isComplete) {
    await removeMetadataFromIngest(task.subject);
    await removeMetadataFromTemp(task.subject);
    const filepath = shareURI.replace("share://", "/share/");
    await deleteFile(filepath);
    return true;
  }
  else {
    return false;
  }
}

/*
 * Removes metadata for the file with the given UUID from the temp graph.
 * Sends a mu-call-scope-id to possibly prevent the deltanotifier from propagating.
 *
 * @private
 * @async
 * @function removeMetaDataFromTemp
 * @param {String} vuuid - Virtual file UUID of the file metadata that needs to be removed
 * @returns {void} Nothing
 */
async function removeMetadataFromTemp(vuuid) {
  const body = bodyArrayToMeta([
    [`?vuri`, `rdf:type`, `nfo:FileDataObject`],
    [`?vuri`, `mu:uuid`, mu.sparqlEscapeString(vuuid)],
    [`?vuri`, `nfo:fileName`, `?filename`],
    [`?vuri`, `dct:format`, `?format`],
    [`?vuri`, `nfo:fileSize`, `?filesize`],
    [`?vuri`, `dbpedia:fileExtension`, `?extension`],
    [`?vuri`, `dct:created`, `?created`],
    [`?vuri`, `dct:modified`, `?modified`],
    [`?puri`, `rdf:type`, `nfo:FileDataObject`],
    [`?puri`, `mu:uuid`, `?puuid`],
    [`?puri`, `nie:dataSource`, `?vuri`],
    [`?puri`, `nfo:fileName`, `?pfilename`],
    [`?puri`, `dct:format`, `?pformat`],
    [`?puri`, `nfo:fileSize`, `?pfilesize`],
    [`?puri`, `dbpedia:fileExtension`, `?pextension`],
    [`?puri`, `dct:created`, `?pcreated`],
    [`?puri`, `dct:modified`, `?pmodified`]
  ]);
  const queryStringOnTemp = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ${body}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ${body}
      }
    }
  `;
  return updateSudo(queryStringOnTemp, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

/*
 * Removes metadata for the file with the given UUID from the ingest graph.
 *
 * @private
 * @async
 * @function removeMetaDataFromIngest
 * @param {String} vuuid - Virtual file UUID of the file metadata that needs to be removed
 * @returns {void} Nothing
 */
async function removeMetadataFromIngest(vuuid) {
  const queryStringOnIngest = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?vuri rdf:type nfo:FileDataObject .
          mu:uuid ?vuuid .
          nfo:fileName ?filename .
          dct:format ?format .
          nfo:fileSize ?filesize .
          dbpedia:fileExtension ?extension .
          dct:created ?created .
          dct:modified ?modified .
        ?new2puri rdf:type nfo:FileDataObject .
          mu:uuid ?puuid .
          nie:dataSource ?vuri .
          nfo:fileName ?pfilename .
          dct:format ?pformat .
          nfo:fileSize ?pfilesize .
          dbpedia:fileExtension ?pextension .
          dct:created ?pcreated .
          dct:modified ?pmodified .
          dct:replaces ?puri .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ${bodyArrayToMeta([
          [`?vuri`, `rdf:type`, `nfo:FileDataObject`],
          [`?vuri`, `mu:uuid`, `?vuuid`],
          [`?vuri`, `nfo:fileName`, `?filename`],
          [`?vuri`, `dct:format`, `?format`],
          [`?vuri`, `nfo:fileSize`, `?filesize`],
          [`?vuri`, `dbpedia:fileExtension`, `?extension`],
          [`?vuri`, `dct:created`, `?created`],
          [`?vuri`, `dct:modified`, `?modified`],
          [`?puri`, `rdf:type`, `nfo:FileDataObject`],
          [`?puri`, `mu:uuid`, `?puuid`],
          [`?puri`, `nie:dataSource`, `?vuri`],
          [`?puri`, `nfo:fileName`, `?pfilename`],
          [`?puri`, `dct:format`, `?pformat`],
          [`?puri`, `nfo:fileSize`, `?pfilesize`],
          [`?puri`, `dbpedia:fileExtension`, `?pextension`],
          [`?puri`, `dct:created`, `?pcreated`],
          [`?puri`, `dct:modified`, `?pmodified`]
        ])}
      }
      BIND(${mu.sparqlEscapeString(vuuid)} AS ?vuuid)
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        OPTIONAL { ?newpuri dct:replaces ?puri . }
      }
      BIND(COALESCE(?newpuri, ?puri) AS ?new2puri)
    }
  `;
  //No mu-call-scope-id because this needs to be propagated through the deltanotifier
  return updateSudo(queryStringOnIngest);
}

/*******************************************************************************
 * Updates
*******************************************************************************/

/*
 * Processes updates to metadata indentified by the task
 *
 * @private
 * @async
 * @function updateFileForTask
 * @param {FileUpdateTask} task - Task that identifies the file URI
 * @returns {void} Nothing
 */
async function updateFileForTask(task) {
  await insertUpdateMetadata(task.subject);
  await removeUpdateMetadata(task.subject);
}

/*
 * Inserts the triples about updates to metadata in the ingest graph and removes old values.
 *
 * @private
 * @async
 * @function insertUpdateMetadata
 * @param {String} uri - URI of the file metadata that needs to be moved
 * @returns {void} Nothing
 */
async function insertUpdateMetadata(uri) {
  const queryStringNoReplacements = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?oldvalue .
      }
    }
    INSERT {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?oldvalue .
      }
    }
  `;
  const queryStringReplacements = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?oldvalue .
      }
    }
    INSERT {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?newuri ?pred ?oldvalue .
        ?newuri dct:replaces ${mu.sparqlEscapeUri(uri)} .
      }
    }
  `;
  //No mu-call-scope-id because this needs to be propagated through the deltanotifier
  await updateSudo(queryStringReplacements); 
  await updateSudo(queryStringNoReplacements);
}

/*
 * Removes the triples about updates to metadata from the temp graph.
 * Sends a mu-call-scope-id to possibly prevent the deltanotifier from propagating.
 *
 * @private
 * @async
 * @function removeUpdateMetadata
 * @param {String} uri - URI of the file metadata that needs to be moved
 * @returns {void} Nothing
 */
async function removeUpdateMetadata(uri) {
  const queryStringNoReplacements = `
    ${PREFIXES}
    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?oldvalue .
      }
    }
  `;
  const queryStringReplacements = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${tripleArrayToMeta([mu.sparqlEscapeUri(uri), `?pred`, `?newvalue`])}
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?newuri ?pred ?oldvalue .
        ?newuri dct:replaces ${mu.sparqlEscapeUri(uri)} .
      }
    }
  `;
  await updateSudo(queryStringReplacements, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC }); 
  await updateSudo(queryStringNoReplacements, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC }); 
}

