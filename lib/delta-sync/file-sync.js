import { query, update, sparqlEscapeUri, sparqlEscapeString } from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { escapeRDFTerm, storeError } from "../utils.js";
import http from "http";
import fs   from "fs";
import path from "path";
import {
  DISABLE_FILE_INGEST,
  PREFIXES,
  TEMP_FILE_GRAPH,
  SYNC_BASE_URL,
  DOWNLOAD_FILE_PATH,
  FILE_FOLDER,
  INGEST_GRAPH,
  TEMP_FILE_REMOVAL_GRAPH,
  REMAPPING
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
    }
    else {
      console.warn('Automated file ingest disabled');
    }
  }
  catch (err) {
    console.error("Synchronisation of files failed with error:", err);
    //Store the error to the database, also include the stacktrace for better debugging (even though the stack trace might be useless when the code changes over time).
    await storeError(err.message + "\n" + err.stack.toString());
  }
}

/**
 * Run the actual file synchronisation. It downloads files, puts them in the correct location, buffers file metadata until all is received, removes files if metadata is deleted, and also buffers this data.
 * Calls different functions to accomplish subtasks.
 *
 * @private
 * @async
 * @function runFileSync
 */
async function runFileSync() {

  ////Scan and process the temporary graph for downloading files

  try {
    //Download files when the vuuid is available
    await downloadFiles();
  }
  catch (err) {
    //Downloads can fail, so log the error, and continue
    for (e of err) {
      console.error(e.message + "\n" + e.stack.toString());
      await storeError(e.message + "\n" + e.stack.toString());
    }
  }

  //Rename and move the files, remap the uri
  try {
    await remapFiles();
  }
  catch (err) {
    //If remapping the file fails, log and continue
    for (let e of err) {
      console.error(e.message + "\n" + e.stack.toString());
      await storeError(e.message + "\n" + e.stack.toString());
    }
  }

  //Move metadata from temp graph to ingest graph. Also remove optional download attempts and success/failure status
  try {
    console.log("Moving metadata that is not needed anymore to the ingest graph");
    await moveFullMetadataToIngest();
  }
  catch (err) {
    //Moving metadata can fail, likely not though. In case of failure, log error and move on.
    console.error(err.message + "\n" + err.stack.toString());
    await storeError(err.message + "\n" + err.stack.toString());
  }

  console.log("Syncing part done: download, remap and store");

  //Scan and process the removal graph for deletes of files

  console.log("Processing removal of files");

  //Query for all the physical filenames the need to be removed
  const pfilepaths = await queryForPFilenames();
  console.log("Files to remove: ", pfilepaths);

  //Remove full file metadata in removal graph that needs to be removed
  await deleteFilesFromTempAndIngest();
  console.log("File metadata removed");

  //Remove files with those names from storage
  for (let filepath of pfilepaths)
    await deleteFile(filepath);
  console.log("Files removed");
}

/*******************************************************************************
 * Subtask functions
*******************************************************************************/

async function downloadFiles() {
  //Find virtual file uuid's (that are not marked as failed)
  let fileVUUIDsAndPURIs = await queryForFileUUIDs();
  console.log("File uuids and physical URIs:", JSON.stringify(fileVUUIDsAndPURIs));
  
  //Download all files from those uuid's if they do not exist already

  //Collect possible errors
  let errors = [];
  let path;

  for (let file of fileVUUIDsAndPURIs) {
    path = file.puri.value.replace("share://", "/share/");
    try {
      await downloadAndSaveFile(file.vuuid.value, path);
      await setDownloadSuccess(file.vuuid.value);
    }
    catch (err) {
      console.warn("Downloading and saving a file failed and is about to throw an error. Will first process this failure for recovering on next tries.");
      await processDownloadFail(file);
      errors.push(new Error(`Downloading and saving file ${file.vuuid.value} failed`, { cause: err }));
    }
  }
  console.log("Files downloaded if that was necessary");
  //If errors, throw all in array
  if (errors.length > 0) {
    throw errors;
  }
}

async function remapFiles() {
  //Query for the filenames, only on full metadata
  const VUUIDsAndPURIs = await queryFullFiles();
  let errors = [];

  //Rename files to their proper name now that all their metadata is received
  for (let zip of VUUIDsAndPURIs) {
    try {
      //Remap the URI and write to the database
      let newpuri = await remapURI(zip.vuuid.value);

      //Only if a new uri actually exists
      if (newpuri) {
        //Move file to new location in accordance to the new URI
        let oldpath = zip.puri.value.replace("share://", "/share/");
        let newpath =        newpuri.replace("share://", "/share/");
        await moveFile(oldpath, newpath);
      }

      //Set renaming, remapping and moving as a success
      await setRemapSuccess(zip.vuuid.value);
    }
    catch (err) {
      console.warn(`Remapping file ${zip.vuuid.value} failed and is throwing an error. Will mark this file as failed. To be ignored later.`);
      await processRemapFail(zip.vuuid.value);
      errors.push(new Error(`Remapping file ${zip.vuuid.value} failed`, { cause: err }));
    }
  }

  if (errors.length > 0) {
    throw errors;
  }
}

async function remapURI(vuuid) {
  //Get the URI of the physical file (e.g. <share://abc.txt>)
  let shareURI = await getPURI(vuuid);

  //Match and replace the uri to the config
  let newShareURI = transformURI(shareURI);
  //If not remapping is necessary, return immediately.
  if (!newShareURI) return false;

  await updatePURI(shareURI, newShareURI);

  return newShareURI;
}

function transformURI(shareURI) {
  let origpath        = shareURI.replace("share://", "/");
  let basename        = path.basename(origpath);
  let dirname         = path.dirname(origpath);
  let remappedDirname = REMAPPING[dirname];
  if (!remappedDirname) return false;
  let newpath         = path.join("/", remappedDirname, basename);
  let newShareURI     = ["share:/", path.normalize(newpath)].join("");
  return newShareURI;
}

/*******************************************************************************
 * Database access
*******************************************************************************/

/**
 * Produce and execute a query on the database that asks for the uuid of virtual files. Only select the ones that have not permanent failure property. This property is set when the download of the file has repeatedly failed.
 *
 * @async
 * @private
 * @function queryForFileUUIDs
 * @return {void} Nothing
 */
async function queryForFileUUIDs() {
  //This query defines the minimum requirement for a file to be downloaded: the uuid for the virtual file
  try {
    let queryString = `
      ${PREFIXES}

      SELECT ?vuuid ?puri
      WHERE {
        GRAPH <${TEMP_FILE_GRAPH}> {
          ?vuri mu:uuid        ?vuuid .
          ?puri nie:dataSource ?vuri  .
          FILTER NOT EXISTS { ?vuri a ext:DownloadFailure .
                              ?vuri a ext:DownloadSuccess . }
        }
      }
    `;

    let results = await querySudo(queryString);
    return results.results.bindings;
  }
  catch (err) {
    throw new Error("Error while querying for UUID's of virtual files in temporary graph.", { cause: err });
  }
}

//Only for complete metadata, and not yet remapped files
async function queryFullFiles() {
  const queryString = `
    ${PREFIXES}

    SELECT ?vuuid ?puri
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a nfo:FileDataObject              ;
              a ext:DownloadSuccess             ;
              mu:uuid               ?vuuid      ;
              nfo:fileName          ?filename   ;
              dct:format            ?format     ;
              nfo:fileSize          ?filesize   ;
              dbpedia:fileExtension ?extension  ;
              dct:created           ?created    ;
              dct:modified          ?modified   .
        ?puri a nfo:FileDataObject              ;
              mu:uuid               ?puuid      ;
              nie:dataSource        ?vuri       ;
              nfo:fileName          ?pfilename  ;
              dct:format            ?pformat    ;
              nfo:fileSize          ?pfilesize  ;
              dbpedia:fileExtension ?pextension ;
              dct:created           ?pcreated   ;
              dct:modified          ?pmodified  .
        FILTER NOT EXISTS { ?vuri a ext:RemapSuccess .
                            ?vuri a ext:RemapFailure . }
      }
    }
  `;
  const results = await querySudo(queryString);
  return results.results.bindings;
}

/**
 * Produce and execute a query on the database in the temporary graph to ask for a complete bundle of metadata of a file. This query does not return anything if not all metadata is present yet.
 *
 * @async
 * @private
 * @function queryFullFiles
 * @return {JSON} Bindings from the result JSON coming from the database
 */
//async function queryFullFiles() {
//  try {
//    let queryString = `
//      ${PREFIXES}
//
//      SELECT *
//      WHERE {
//        GRAPH <${TEMP_FILE_GRAPH}> {
//          ?vuri a nfo:FileDataObject              ;
//                mu:uuid               ?vuuid      ;
//                nfo:fileName          ?filename   ;
//                dct:format            ?format     ;
//                nfo:fileSize          ?filesize   ;
//                dbpedia:fileExtension ?extension  ;
//                dct:created           ?created    ;
//                dct:modified          ?modified   .
//          ?puri a nfo:FileDataObject              ;
//                mu:uuid               ?puuid      ;
//                nie:dataSource        ?vuri       ;
//                nfo:fileName          ?pfilename  ;
//                dct:format            ?pformat    ;
//                nfo:fileSize          ?pfilesize  ;
//                dbpedia:fileExtension ?pextension ;
//                dct:created           ?pcreated   ;
//                dct:modified          ?pmodified  .
//        }
//      }
//    `;
//    let results = await querySudo(queryString);
//    return results.results.bindings;
//  }
//  catch (err) {
//    throw new Error("Error while querying for all metadata in one piece of a file in the temporary graph.", { cause: err });
//  }
//}

/**
 * Store the full metadata of a file in a specific graph.
 *
 * @async
 * @private
 * @function storeFullFile
 * @param {JSON} fileMetadata - Metadata to be stored, same format as what is returned from the queryFullFiles function
 * @param {string} method - Give either INSERT of DELETE to insert or delete the metadata respectively
 * @param {string} graph - Graph in which to store the metadata
 * @return {void} Nothing
 */
//async function storeFullFile(fileMetadata, method, graph) {
//  try {
//    method = method || "INSERT";
//    graph  = graph  || INGEST_GRAPH;
//    console.log("Storing full file: ", fileMetadata);
//    const fm = fileMetadata;
//    let queryString = `
//      ${PREFIXES}
//
//      ${method} DATA {
//        GRAPH <${graph}> {
//          ${escapeRDFTerm(fm.vuri)} a nfo:FileDataObject              ;
//            mu:uuid                   ${escapeRDFTerm(fm.vuuid)}      ;
//            nfo:fileName              ${escapeRDFTerm(fm.filename)}   ;
//            dct:format                ${escapeRDFTerm(fm.format)}     ;
//            nfo:fileSize              ${escapeRDFTerm(fm.filesize)}   ;
//            dbpedia:fileExtension     ${escapeRDFTerm(fm.extension)}  ;
//            dct:created               ${escapeRDFTerm(fm.created)}    ;
//            dct:modified              ${escapeRDFTerm(fm.modified)}   .
//          ${escapeRDFTerm(fm.puri)} a nfo:FileDataObject              ;
//            mu:uuid                   ${escapeRDFTerm(fm.puuid)}      ;
//            nie:dataSource            ${escapeRDFTerm(fm.vuri)}       ;
//            nfo:fileName              ${escapeRDFTerm(fm.pfilename)}  ;
//            dct:format                ${escapeRDFTerm(fm.pformat)}    ;
//            nfo:fileSize              ${escapeRDFTerm(fm.pfilesize)}  ;
//            dbpedia:fileExtension     ${escapeRDFTerm(fm.pextension)} ;
//            dct:created               ${escapeRDFTerm(fm.pcreated)}   ;
//            dct:modified              ${escapeRDFTerm(fm.pmodified)}  .
//        }
//      }
//    `;
//
//    return updateSudo(queryString);
//  }
//  catch (err) {
//    throw new Error(`Error while storing/deleting all metadata of a file in graph ${graph}.`, { cause: err });
//  }
//}

/**
 * Produce and execute a query on the database in the temporary removal graph to ask for the filenames of physical files. This only selects UUIDs for files where the metadata is complete, and takes care to deal with replacements that came from remapping.
 *
 * @async
 * @private
 * @function queryForPFilenames
 * @return {JSON} Bindings about filenames that need to be removed.
 */
async function queryForPFilenames() {
  try {
    let queryString = `
      ${PREFIXES}

      SELECT ?new2puri
      WHERE {
        GRAPH <${TEMP_FILE_REMOVAL_GRAPH}> {
          ?vuri a nfo:FileDataObject              ;
                mu:uuid               ?vuuid      ;
                nfo:fileName          ?filename   ;
                dct:format            ?format     ;
                nfo:fileSize          ?filesize   ;
                dbpedia:fileExtension ?extension  ;
                dct:created           ?created    ;
                dct:modified          ?modified   .
          ?puri a nfo:FileDataObject              ;
                mu:uuid               ?puuid      ;
                nie:dataSource        ?vuri       ;
                nfo:fileName          ?pfilename  ;
                dct:format            ?pformat    ;
                nfo:fileSize          ?pfilesize  ;
                dbpedia:fileExtension ?pextension ;
                dct:created           ?pcreated   ;
                dct:modified          ?pmodified  .
        }
        GRAPH <${INGEST_GRAPH}> {
          OPTIONAL { ?newpuri dct:replaces ?puri }
        }
        BIND(COALESCE(?newpuri, ?puri) AS ?new2puri)
      }
    `;
    let results = await querySudo(queryString);
    let paths   = results.results.bindings.map(b => b.new2puri.value.replace("share://", "/share/"));
    return paths;
  }
  catch (err) {
    throw new Error("Error while querying for filenames of physical files in the temporary removal graph.", { cause: err });
  }
}

async function moveFullMetadataToIngest() {
  let queryString = `
    ${PREFIXES}

    DELETE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a nfo:FileDataObject              ;
              a ext:DownloadSuccess             ;
              a ext:RemapSuccess                ;
              mu:uuid               ?vuuid      ;
              nfo:fileName          ?filename   ;
              dct:format            ?format     ;
              nfo:fileSize          ?filesize   ;
              dbpedia:fileExtension ?extension  ;
              dct:created           ?created    ;
              dct:modified          ?modified   .
        ?puri a nfo:FileDataObject              ;
              mu:uuid               ?puuid      ;
              nie:dataSource        ?vuri       ;
              nfo:fileName          ?pfilename  ;
              dct:format            ?pformat    ;
              nfo:fileSize          ?pfilesize  ;
              dbpedia:fileExtension ?pextension ;
              dct:created           ?pcreated   ;
              dct:modified          ?pmodified  ;
              dct:replaces          ?oldpuri    .
      }
    }
    INSERT {
      GRAPH <${INGEST_GRAPH}> {
        ?vuri a nfo:FileDataObject              ;
              mu:uuid               ?vuuid      ;
              nfo:fileName          ?filename   ;
              dct:format            ?format     ;
              nfo:fileSize          ?filesize   ;
              dbpedia:fileExtension ?extension  ;
              dct:created           ?created    ;
              dct:modified          ?modified   .
        ?puri a nfo:FileDataObject              ;
              mu:uuid               ?puuid      ;
              nie:dataSource        ?vuri       ;
              nfo:fileName          ?pfilename  ;
              dct:format            ?pformat    ;
              nfo:fileSize          ?pfilesize  ;
              dbpedia:fileExtension ?pextension ;
              dct:created           ?pcreated   ;
              dct:modified          ?pmodified  ;
              dct:replaces          ?oldpuri    .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a nfo:FileDataObject              ;
              a ext:DownloadSuccess             ;
              a ext:RemapSuccess                ;
              mu:uuid               ?vuuid      ;
              nfo:fileName          ?filename   ;
              dct:format            ?format     ;
              nfo:fileSize          ?filesize   ;
              dbpedia:fileExtension ?extension  ;
              dct:created           ?created    ;
              dct:modified          ?modified   .
        ?puri a nfo:FileDataObject              ;
              mu:uuid               ?puuid      ;
              nie:dataSource        ?vuri       ;
              nfo:fileName          ?pfilename  ;
              dct:format            ?pformat    ;
              nfo:fileSize          ?pfilesize  ;
              dbpedia:fileExtension ?pextension ;
              dct:created           ?pcreated   ;
              dct:modified          ?pmodified  .
        OPTIONAL { ?puri dct:replaces ?oldpuri . }
      }
    }
    `;
  //This is query is shorter, and more readable, but takes too long to execute, but is otherwise identical in functionality.
    //DELETE {
    //  GRAPH <${TEMP_FILE_GRAPH}> {
    //    ?puri ?pp ?op .
    //    ?vuri ?pv ?ov .
    //  }
    //}
    //INSERT {
    //  GRAPH <${INGEST_GRAPH}> {
    //    ?puri ?pp ?op .
    //    ?vuri ?pv ?ov .
    //  }
    //}
    //WHERE {
    //  GRAPH <${TEMP_FILE_GRAPH}> {
    //    ?vuri a nfo:FileDataObject              ;
    //          a ext:DownloadSuccess             ;
    //          a ext:RemapSuccess                ;
    //          mu:uuid               ?vuuid      ;
    //          nfo:fileName          ?filename   ;
    //          dct:format            ?format     ;
    //          nfo:fileSize          ?filesize   ;
    //          dbpedia:fileExtension ?extension  ;
    //          dct:created           ?created    ;
    //          dct:modified          ?modified   .
    //    ?puri a nfo:FileDataObject              ;
    //          mu:uuid               ?puuid      ;
    //          nie:dataSource        ?vuri       ;
    //          nfo:fileName          ?pfilename  ;
    //          dct:format            ?pformat    ;
    //          nfo:fileSize          ?pfilesize  ;
    //          dbpedia:fileExtension ?pextension ;
    //          dct:created           ?pcreated   ;
    //          dct:modified          ?pmodified  .
    //    OPTIONAL { ?puri dct:replaces ?oldpuri . }
    //    ?puri ?pp ?op .
    //    ?vuri ?pv ?ov .
    //  }
    //}

  try {
    return updateSudo(queryString);
  }
  catch (err) {
    throw new Error("Error while moving the finished metadata to the ingest graph", { cause: err });
  }
}

/**
 * Produce and execute a query on the database that removes complete metadata from the ingest graph if it exists in the temporary removal graph.
 *
 * @async
 * @private
 * @function deleteFilesFromTempAndIngest
 * @return {void} Nothing
 */
async function deleteFilesFromTempAndIngest() {
  try {
    let queryString = `
      ${PREFIXES}

      DELETE {
        GRAPH <${INGEST_GRAPH}> {
          ?vuri a nfo:FileDataObject              ;
                mu:uuid               ?vuuid      ;
                nfo:fileName          ?filename   ;
                dct:format            ?format     ;
                nfo:fileSize          ?filesize   ;
                dbpedia:fileExtension ?extension  ;
                dct:created           ?created    ;
                dct:modified          ?modified   .
          ?new2puri a nfo:FileDataObject              ;
                    mu:uuid               ?puuid      ;
                    nie:dataSource        ?vuri       ;
                    nfo:fileName          ?pfilename  ;
                    dct:format            ?pformat    ;
                    nfo:fileSize          ?pfilesize  ;
                    dbpedia:fileExtension ?pextension ;
                    dct:created           ?pcreated   ;
                    dct:modified          ?pmodified  .
          ?new2puri dct:replaces ?puri .
        }
        GRAPH <${TEMP_FILE_REMOVAL_GRAPH}> {
          ?vuri a nfo:FileDataObject              ;
                mu:uuid               ?vuuid      ;
                nfo:fileName          ?filename   ;
                dct:format            ?format     ;
                nfo:fileSize          ?filesize   ;
                dbpedia:fileExtension ?extension  ;
                dct:created           ?created    ;
                dct:modified          ?modified   .
          ?puri a nfo:FileDataObject              ;
                mu:uuid               ?puuid      ;
                nie:dataSource        ?vuri       ;
                nfo:fileName          ?pfilename  ;
                dct:format            ?pformat    ;
                nfo:fileSize          ?pfilesize  ;
                dbpedia:fileExtension ?pextension ;
                dct:created           ?pcreated   ;
                dct:modified          ?pmodified  .
        }
      }
      WHERE {
        GRAPH <${TEMP_FILE_REMOVAL_GRAPH}> {
          ?vuri a nfo:FileDataObject              ;
                mu:uuid               ?vuuid      ;
                nfo:fileName          ?filename   ;
                dct:format            ?format     ;
                nfo:fileSize          ?filesize   ;
                dbpedia:fileExtension ?extension  ;
                dct:created           ?created    ; 
                dct:modified          ?modified   . 
          ?puri a nfo:FileDataObject              ; 
                mu:uuid               ?puuid      ; 
                nie:dataSource        ?vuri       ; 
                nfo:fileName          ?pfilename  ; 
                dct:format            ?pformat    ; 
                nfo:fileSize          ?pfilesize  ; 
                dbpedia:fileExtension ?pextension ; 
                dct:created           ?pcreated   ; 
                dct:modified          ?pmodified  . 
        }
        GRAPH <${INGEST_GRAPH}> {
          OPTIONAL { ?newpuri dct:replaces ?puri . }
        }
        BIND(COALESCE(?newpuri, ?puri) AS ?new2puri)
      }
    `;
    return updateSudo(queryString);
  }
  catch (err) {
    throw new Error("Error while moving file metadata from the temporary to the ingest graph.", { cause: err });
  }
}

async function processDownloadFail(vuuid) {
  const previousAttempts = await queryPreviousDownloadAttempts(vuuid);
  const attempts = previousAttempts + 1;

  if (attempts > MAX_FAIL_DOWNLOAD_ATTEMPTS) {
    //Too many attempts, move all metadata to failure graph next time by setting the download to permanently failed
    await setDownloadFailed(vuuid);
  }
  else {
    //Still more attempts possible, increase attemptcount
    await setDownloadAttemptCount(vuuid, attempts);
  }
}

async function queryPreviousDownloadAttempts(vuuid) {
  const queryString = `
    ${PREFIXES}

    SELECT ?attemptCount
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
        ?vuri ext:downloadAttempt ?attemptCount .
      }
    }
  `;
  const results = querySudo(queryString);
  if (results.results.bindings.length > 0)
    return results.results.bindings[0].attemptCount.value;
  else
    return 0;
}

async function setDownloadFailed(vuuid) {
  return updateSudo(`
    ${PREFIXES}

    DELETE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri ext:downloadAttempt ?attemptCount .
      }
    }
    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a ext:DownloadFailure .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
        ?vuri ext:downloadAttempt ?attemptCount .
      }
    }
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function processRemapFail(vuuid) {
  return updateSudo(`
    ${PREFIXES}

    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a ext:RemapFailure .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
      }
    }
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function setDownloadSuccess(vuuid) {
  return updateSudo(`
    ${PREFIXES}

    DELETE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri ext:downloadAttempt ?attemptCount .
      }
    }
    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a ext:DownloadSuccess .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
        OPTIONAL { ?vuri ext:downloadAttempt ?attemptCount . }
      }
    }
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function setDownloadAttemptCount(vuuid, attempts) {
  return updateSudo(`
    ${PREFIXES}

    DELETE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri ext:downloadAttempt ?attemptCount .
      }
    }
    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri ext:downloadAttempt ${attempts} .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
        OPTIONAL { ?vuri ext:downloadAttempt ?attemptCount . }
      }
    }
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function setRemapSuccess(vuuid) {
  return updateSudo(`
    ${PREFIXES}

    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a ext:RemapSuccess .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
      }
    }
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
}

async function getPURI(vuuid) {
  let queryString = `
    ${PREFIXES}

    SELECT ?puri
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid ${sparqlEscapeString(vuuid)} .
        ?puri nie:dataSource ?vuri .
      }
    }
  `;
  let results = await querySudo(queryString);
  if (results.results.bindings.length > 0)
    return results.results.bindings[0].puri.value;
}

//Change old uri in new uri for physical files. Also insert a replacement triple.
async function updatePURI(olduri, newuri) {
  let queryString = `
    ${PREFIXES}

    DELETE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ${sparqlEscapeUri(olduri)} ?p ?o .
      }
    }
    INSERT {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ${sparqlEscapeUri(newuri)} ?p ?o .
        ${sparqlEscapeUri(newuri)} dct:replaces ${sparqlEscapeUri(olduri)} .
      }
    }
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ${sparqlEscapeUri(olduri)} ?p ?o .
      }
    }
  `;
  try {
    let res = await updateSudo(queryString, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_FILE_SYNC });
    return res;
  }
  catch (err) {
    console.error("Failure during updating the URI: ", err.message);
    throw new Error(`Failure during updating the URI to ${newuri}`, { cause: err });
  }
}

/*******************************************************************************
 * File download and storage
*******************************************************************************/

/**
 * Download a file from the producer and put it in the correct path, on the shared volume
 *
 * @async
 * @private
 * @function downloadAndSaveFile
 * @param {string} vuuid - The UUID of the virtual file to be downloaded.
 * @return {void} Nothing
 */
async function downloadAndSaveFile(vuuid, filepath) {

  if (!filepath.startsWith(FILE_FOLDER)) {
    console.error(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}.`);
    throw new Error(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}. Files can get lost like this!`);
  }

  //The options for the http request to the producer
  let options = {
    method:  "GET",
    host:    SYNC_BASE_URL.replace("http://", "").replace("/", ""),
    path:    DOWNLOAD_FILE_PATH.replace(":id", vuuid),
    port:    80,
  };

  console.log("Will download the file with uuid", vuuid);

  return new Promise((resolve, reject) => {

    //First make sure the destination path exists
    let targetdir = path.dirname(filepath);
    fs.mkdir(targetdir, { recursive: true }, err => {
      if (err) {
        console.error(err);
        reject(new Error(`Creating target directory ${targetdir} for file download failed.`));
      }
      resolve();
    });
  })
  .then(() => new Promise((resolve, reject) => {

    //Setup the write stream for a local file
    //FOR NODE 16 and up:
      //let writeFileHandle = await fs.open(`${puuid}.${ext}`, "wx");
      //let writeStream     = await writeFileHandle.createWriteStream();
    //FOR NODE 14:
    let writeStream = fs.createWriteStream(filepath, { flags: "wx" });
    //You would want to use flags wx to prevent file from being overwritten
    console.log("Opening a writeStream");
    //Close the file properly
    //FOR NODE 16 and up:
      //writeFileHandle.on("finish", () => writeFileHandle.close());
    //FOR NODE 14:
    writeStream.on("finish", () => {
      console.log("Closing the writeStream");
      writeStream.close();
    });
    writeStream.on("error", async (err) => {
      if (err.errno == -17) {
        console.log("File already exists on local storage, ignoring and not downloading the file again");
        resolve();
      } else {
        //When error, don't leave incomplete file around
        writeStream.close();
        await deleteFile(filepath);
        reject(new Error(`Error while downloading and saving file ${vuuid} on the consumer, during writing to the filestream.`, { cause: err }));
      }
    });
    writeStream.on("ready", () => {

      //Setup the http request
      let req = http.request(options);

      req.on("response", (res) => {
        console.log("Inside the callback of the request.");
        //On request finishing, pipe data directly to a file
        res.pipe(writeStream);
        console.log("Piped the streams");

        res.on("error", (err) => {
          console.error("Error on the response for file download", err);
          console.log("Closing the writeStream");
          writeStream.close();
          reject(new Error(`Error while downloading and saving file ${vuuid} on the consumer, during the request of the file to the producer file service.`, { cause: err }));
        });

        res.on("end", () => {
          console.log("End of the request reached with statuscode: ", res.statusCode);
          console.log("Closing the writeStream");
          writeStream.close();
          resolve();
        });
      });

      req.on("timeout", (err) => {
        console.error(err);
        req.destroy();
        reject(new Error(`Error while downloading and saving file ${vuuid} on the consumer, due to a timeout on the request to the producer file service.`, { cause: err }));
      });

      req.on("end", () => {
        resolve();
      });

      //Send the request
      req.end();
    });
  }));
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
      if (err) reject(new Error(`Error while renaming file ${oldpath} into ${newpath}.`, { cause: err }));
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
          reject(new Error(`Error while deleting file ${filepath}.`, { cause: err }))
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
        reject(new Error(`Creating directory ${targetdir} failed.`, { cause: err }));
      }
      resolve();
    });
  }).then(() => {
    
    return new Promise((resolve, reject) => {
      fs.rename(oldfilepath, newfilepath, (err) => {
        if (err) {
          console.error(err);
          reject(new Error(`Moving the file from ${oldfilepath} to ${newfilepath} failed.`, { cause: err }));
        }
        resolve();
      });
    });
  });
}

