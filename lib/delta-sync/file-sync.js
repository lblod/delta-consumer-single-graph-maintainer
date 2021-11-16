import * as mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { escapeRDFTerm, storeError } from "../utils.js";
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
 * Start the synchronisation of files. Scan the temporary graph, download files and push file metadata to the rest of the application.
 * Runs different functions to accomplish subtasks.
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

async function runFileSync() {
  //Collect tasks that are ongoing
  const ongoingFileAddTasks = await getOngoingFileAddTasks();
  //Create tasks for new virtual file, excluding the already ongoing tasks
  const fileAddTasks = await scheduleNewFileAddTasks(ongoingFileAddTasks);
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
        await finishFileAddTask(task);
      }
    }

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
      await finishFileRemoveTask(task);
    }
  }

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

  //TODO
  //await cleanupAfterFailures();
}

/*******************************************************************************
 * Subtask functions
*******************************************************************************/

async function startFileAddTask(task) {
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

async function downloadFileDuringTask(task) {
  switch (task.downloadStatus) {
    case DOWNLOAD_NOT_STARTED_STATUS:
      await task.setDownloadOngoing();
      return downloadFileDuringTask(task);
    case DOWNLOAD_ONGOING_STATUS:
      //Perform another download attempt
      try {
        await downloadFileForTask(task);
        await task.madeDownloadAttempt();
        await task.setDownloadSuccess();
      }
      catch (err) {
        await task.madeDownloadAttempt();
        throw err;
      }
      break;
    case DOWNLOAD_FAILURE_STATUS:
    case DOWNLOAD_SUCCESS_STATUS:
      //Don't do anything
      break;
    default:
      throw Error(`Task on ${task.muuuid} has an unknown download status of ${task.downloadStatus}`);
  }
}

async function remapFileMetadata(task) {
  if (task.downloadStatus != DOWNLOAD_SUCCESS_STATUS)
    return;

  switch (task.remappingStatus) {
    case REMAPPING_NOT_STARTED_STATUS:
      await task.setRemappingOngoing();
      return remapFileMetadata(task);
    case REMAPPING_ONGOING_STATUS:
      try {
        const succeeded = await remapFileForTask(task);
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

async function moveFileMetadata(task) {
  if (task.downloadStatus != DOWNLOAD_SUCCESS_STATUS || task.remappingStatus != REMAPPING_SUCCESS_STATUS)
    return;

  switch (task.movingStatus) {
    case MOVING_NOT_STARTED_STATUS:
      await task.setMovingOngoing();
      return moveFileMetadata(task);
    case MOVING_ONGOING_STATUS:
      try {
        const succeeded = await moveFileMetadataForTask(task);
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

async function finishFileAddTask(task) {
  if (task.downloadStatus === DOWNLOAD_SUCCESS_STATUS && task.remappingStatus === REMAPPING_SUCCESS_STATUS && task.movingStatus === MOVING_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  if (task.downloadStatus === DOWNLOAD_FAILURE_STATUS || task.remappingStatus === REMAPPING_FAILURE_STATUS || task.movingStatus === MOVING_FAILURE_STATUS)
    await task.setStatus(TASK_FAILED_STATUS);
}

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

async function removeFileDuringTask(task) {
  switch (task.removeStatus) {
    case REMOVE_NOT_STARTED_STATUS:
      await task.setRemoveOngoing();
      return removeFileDuringTask(task);
    case REMOVE_ONGOING_STATUS:
      try {
        let succeeded = await removeFileForTask(task);
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

async function finishFileRemoveTask(task) {
  if (task.removeStatus === REMOVE_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  if (task.removeStatus === REMOVE_FAILURE_STATUS)
    await task.setStatus(TASK_FAILED_STATUS);
}

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

async function finishFileUpdateTask(task) {
  if (task.updateStatus === UPDATE_SUCCESS_STATUS)
    await task.setStatus(TASK_SUCCESS_STATUS);
  if (task.updateStatus === UPDATE_FAILURE_STATUS)
    await task.setStatus(TASK_FAILED_STATUS);
}

/*******************************************************************************
 * Scheduling tasks
*******************************************************************************/

async function getOngoingFileAddTasks() {
  let tasks = [];

  const queryString = `
    ${PREFIXES}

    SELECT * {
      GRAPH ${mu.sparqlEscapeUri(JOBS_GRAPH)} {
        ?uri
          a ext:SyncTask ;
          a ext:FileAddTask ;
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

async function scheduleNewFileAddTasks(alreadyOngoingTasks) {
  const queryString = `
    ${PREFIXES}

    SELECT ?vuuid
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ?vuri mu:uuid ?vuuid .
        ?puri nie:dataSource ?vuri  .
      }
    }
  `;
  const vuuids = (await querySudo(queryString)).results.bindings.map(b => b.vuuid.value);
  console.log("Vuuids of files to add", vuuids);
  let newTask;
  let newTasks = [];
  for (let vuuid of vuuids) {
    if (!alreadyOngoingTasks.some(t => t.subject == vuuid)) {
      newTask = new FileAddTask(vuuid);
      await newTask.persist();
      newTasks.push(newTask);
    }
  }
  console.log("Tasks scheduled");
  return newTasks;
}

async function scheduleNewFileRemoveTasks() {
  const queryString = `
    ${PREFIXES}

    SELECT ?vuuid
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ?vuuid ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
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

async function scheduleNewFileUpdateTasks() {
  const queryStringNoReplacements = `
    ${PREFIXES}

    SELECT ?uri {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ?uri ?pred ?newval .
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
        ?uri ?pred ?newval .
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

async function getPURIFromVUUID(vuuid, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const queryString = `
    ${PREFIXES}
    
    SELECT ?puri {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ?vuri mu:uuid ${mu.sparqlEscapeString(vuuid)} .
        ?puri nie:dataSource ?vuri  .
      }
    }
  `;
  const response = await querySudo(queryString);
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
  let queryString = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(olduri)} ?p ?o .
      }
    }
    INSERT {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(newuri)} ?p ?o .
        ${mu.sparqlEscapeUri(newuri)} dct:replaces ${mu.sparqlEscapeUri(olduri)} .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(olduri)} ?p ?o .
      }
    }
  `;
  return await updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function isMetadataFull(vuuid, graph) {
  graph = graph || TEMP_FILE_GRAPH;
  const queryString = `
    ${PREFIXES}

    SELECT * {
      GRAPH ${mu.sparqlEscapeUri(graph)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
        OPTIONAL { ?puri dct:replaces ?oldpuri . }
      }
    }
  `;
  const response = await querySudo(queryString);
  if (response.results.bindings.length > 0)
    return true;
  else
    return false;
}

/*******************************************************************************
 * File download and storage
*******************************************************************************/

async function downloadFileForTask(task) {
  const puri = await getPURIFromVUUID(task.subject);
  const path = puri.replace("share://", "/share/");
  return await downloadAndSaveFile(task.subject, path);
}

/**
 * Download a file from the producer and put it in the given path, on the shared volume
 *
 * @async
 * @private
 * @function downloadAndSaveFile
 * @param {string} vuuid - The UUID of the virtual file to be downloaded
 * @param {string} filepath - The file path, with name included, of the destination of the download
 * @return {void} Nothing
 */
async function downloadAndSaveFile(vuuid, filepath) {

  if (!filepath.startsWith(FILE_FOLDER)) {
    throw new Error(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}. Files can get lost like this!`);
  }

  //The options for the http request to the producer
  const fileDownloadURL = SYNC_BASE_URL.concat(DOWNLOAD_FILE_ENDPOINT.replace(":id", vuuid));

  try {
    await createTargetDir(filepath);
    let writeStream = await createWriteStream(filepath);
    let response = await fetch(fileDownloadURL);
    return writeStream.pipe(response);
  }
  catch (err) {
    await deleteFile(filepath);
    throw err;
  }
}

async function createTargetDir(filepath) {
  let targetdir = path.dirname(filepath);
  fs.mkdir(targetdir, { recursive: true }, err => {
    if (err) {
      throw new Error(`Target directory ${targetdir} creation failed`);
    }
  });
}

function createWriteStream(filepath) {
  return new Promise((resolve, reject) => {
    let writeStream = fs.createWriteStream(filepath, { flags: "wx" });
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
  console.log("Removing file with path:", filepath);
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
  console.log(`Moving file from ${oldfilepath} to ${newfilepath}`);

  return new Promise((resolve, reject) => {
    let targetdir = path.dirname(newfilepath);
    console.log("Creating directory if not exists:", targetdir);
    fs.mkdir(targetdir, { recursive: true }, (err) => {
      if (err) {
        console.error(err);
        reject(new StackableError(`Creating directory ${targetdir} failed.`, err));
      }
      resolve();
    });
  }).then(() => {
    
    return new Promise((resolve, reject) => {
      fs.rename(oldfilepath, newfilepath, (err) => {
        if (err) {
          console.error(err);
          reject(new StackableError(`Moving the file from ${oldfilepath} to ${newfilepath} failed.`, err));
        }
        resolve();
      });
    });
  });
}

/*******************************************************************************
 * Remapping
*******************************************************************************/

async function remapFileForTask(task) {
  const isComplete = await isMetadataFull(task.subject);
  if (isComplete) {
    const shareURI = await getPURIFromVUUID(task.subject);
    const newShareURI = transformURI(shareURI);
    //If no remapping is necessary, return immediately.
    if (!newShareURI) return true;

    const oldpath = shareURI.replace("share://", "/share/");
    const newpath = newShareURI.replace("share://", "/share/");
    await moveFile(oldpath, newpath);

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
 * @param {string} shareURI - URI of the physical file that needs to be remapped
 * @return {(boolean|string)} Returns either a string with the new URI or false to indicate that no remapping is necessary.
 */
function transformURI(shareURI) {
  let origpath = shareURI.replace("share://", "/");
  let basename = path.basename(origpath);
  let dirname = path.dirname(origpath);
  let remappedDirname = REMAPPING[dirname];
  if (!remappedDirname) return false;
  let newpath = path.join("/", remappedDirname, basename);
  let newShareURI = ["share:/", path.normalize(newpath)].join("");
  return newShareURI;
}

/*******************************************************************************
 * Moving
*******************************************************************************/

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

async function moveFullMetadataDelete(vuuid) {
  const queryString = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
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
        ?vuri a nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
        OPTIONAL { ?puri dct:replaces ?oldpuri . }
      }
    }
  `;
  return updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function moveFullMetadataToIngest(vuuid) {
  const queryString = `
    ${PREFIXES}

    INSERT {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
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
        ?vuri a nfo:FileDataObject ;
          mu:uuid ${mu.sparqlEscapeString(vuuid)} ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
        OPTIONAL { ?puri dct:replaces ?oldpuri . }
      }
    }
  `;
  return updateSudo(queryString);
}

/*******************************************************************************
 * Removing
*******************************************************************************/

async function removeFileForTask(task) {
  let shareURI = await getPURIFromVUUID(task.subject, TEMP_FILE_REMOVAL_GRAPH);
  let isComplete = await isMetadataFull(task.subject, TEMP_FILE_REMOVAL_GRAPH);
  if (isComplete) {
    await removeMetadataFromIngest(task.subject);
    await removeMetadataFromTemp(task.subject);
    let filepath = shareURI.replace("share://", "/share/");
    await deleteFile(filepath);
    return true;
  }
  else {
    return false;
  }
}

async function removeMetadataFromTemp(vuuid) {
  const queryStringOnTemp = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ?vuuid ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ?vuuid ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
      }
      BIND(${mu.sparqlEscapeString(vuuid)} AS ?vuuid)
    }
  `;
  return updateSudo(queryStringOnTemp, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function removeMetadataFromIngest(vuuid) {
  const queryStringOnIngest = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ?vuuid ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?new2puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
        ?new2puri dct:replaces ?puri .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_REMOVAL_GRAPH)} {
        ?vuri a nfo:FileDataObject ;
          mu:uuid ?vuuid ;
          nfo:fileName ?filename ;
          dct:format ?format ;
          nfo:fileSize ?filesize ;
          dbpedia:fileExtension ?extension ;
          dct:created ?created ;
          dct:modified ?modified .
        ?puri a nfo:FileDataObject ;
          mu:uuid ?puuid ;
          nie:dataSource ?vuri ;
          nfo:fileName ?pfilename ;
          dct:format ?pformat ;
          nfo:fileSize ?pfilesize ;
          dbpedia:fileExtension ?pextension ;
          dct:created ?pcreated ;
          dct:modified ?pmodified .
      }
      BIND(${mu.sparqlEscapeString(vuuid)} AS ?vuuid)
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        OPTIONAL { ?newpuri dct:replaces ?puri . }
      }
      BIND(COALESCE(?newpuri, ?puri) AS ?new2puri)
    }
  `;
  return updateSudo(queryStringOnIngest);
}

/*******************************************************************************
 * Updates
*******************************************************************************/

async function updateFileForTask(task) {
  await insertUpdateMetadata(task.subject);
  await removeUpdateMetadata(task.subject);
}

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
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
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
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
      }
      GRAPH ${mu.sparqlEscapeUri(INGEST_GRAPH)} {
        ?newuri ?pred ?oldvalue .
        ?newuri dct:replaces ${mu.sparqlEscapeUri(uri)} .
      }
    }
  `;
  await updateSudo(queryStringReplacements); 
  await updateSudo(queryStringNoReplacements);
}

async function removeUpdateMetadata(uri) {
  const queryStringNoReplacements = `
    ${PREFIXES}

    DELETE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
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
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
      }
    }
    WHERE {
      GRAPH ${mu.sparqlEscapeUri(TEMP_FILE_GRAPH)} {
        ${mu.sparqlEscapeUri(uri)} ?pred ?newvalue .
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

