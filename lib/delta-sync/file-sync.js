import { query, update } from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { escapeRDFTerm } from "../utils.js";
import http from "http";
import fs   from "fs";
import {
  DISABLE_FILE_INGEST,
  PREFIXES,
  TEMP_FILE_GRAPH,
  SYNC_BASE_URL,
  DOWNLOAD_FILE_PATH,
  FILE_FOLDER,
  INGEST_GRAPH
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
  }
}

/**
 * Run the actual file synchronisation. Calls different functions to accomplish subtasks.
 *
 * @private
 * @async
 * @function runFileSync
 */
async function runFileSync() {
  console.log("Welcome in the file sync");
  let fileVuuids = await queryForFileUUIDs();
  console.log("File uuids:", JSON.stringify(fileVuuids));
  fileVuuids = fileVuuids.map(vuuid => vuuid.vuuid.value);
  const fileDownloadPs = fileVuuids.map(downloadAndSaveFile);
  await Promise.all(fileDownloadPs);
  console.log("Files downloaded if that was necessary");
  const fullFiles = await queryFullFiles();
  console.log("Found full files:", JSON.stringify(fullFiles));
  const renameFilePs = fullFiles.map(file => renameFile(file.vuuid.value, file.pfilename.value));
  await Promise.all(renameFilePs);
  let fullFileStorePs = fullFiles.map(storeFullFile);
  await Promise.all(fullFileStorePs);
}

/*******************************************************************************
 * Database access
*******************************************************************************/

async function queryForFileUUIDs() {
  //This query defines the minimum requirement for a file to be downloaded: the uuid for the virtual file
  let queryString = `
    ${PREFIXES}

    SELECT ?vuuid
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri mu:uuid        ?vuuid .
        ?puri nie:dataSource ?vuri  .
      }
    }
  `;

  let results = await querySudo(queryString);
  return results.results.bindings;
}

async function queryFullFiles() {
  let queryString = `
    ${PREFIXES}

    SELECT *
    WHERE {
      GRAPH <${TEMP_FILE_GRAPH}> {
        ?vuri a nfo:FileDataObject              ;
              mu:uuid               ?vuuid      ;
              nfo:fileName          ?filename   ;
              dct:format            ?format     ;
              dbpedia:fileExtension ?extension  ;
              dct:created           ?created    ;
              dct:modified          ?modified   .
        ?puri a nfo:FileDataObject              ;
              mu:uuid               ?puuid      ;
              nie:dataSource        ?vuri       ;
              nfo:fileName          ?pfilename  ;
              dct:format            ?pformat    ;
              dbpedia:fileExtension ?pextension ;
              dct:created           ?pcreated   ;
              dct:modified          ?pmodified  .
      }
    }
  `;
  let results = await querySudo(queryString);
  return results.results.bindings;
}

async function storeFullFile(fileMetadata) {
  console.log("Storing full file: ", fileMetadata);
  const fm = fileMetadata;
  let queryString = `
    ${PREFIXES}

    INSERT DATA {
      GRAPH <${INGEST_GRAPH}> {
        ${escapeRDFTerm(fm.vuri)} a nfo:FileDataObject ;
          mu:uuid                   ${escapeRDFTerm(fm.vuuid)} ;
          nfo:fileName              ${escapeRDFTerm(fm.filename)} ;
          dct:format                ${escapeRDFTerm(fm.format)} ;
          dbpedia:fileExtension     ${escapeRDFTerm(fm.extension)} ;
          dct:created               ${escapeRDFTerm(fm.created)} ;
          dct:modified              ${escapeRDFTerm(fm.modified)} .
        ${escapeRDFTerm(fm.puri)} a nfo:FileDataObject ;
          mu:uuid                   ${escapeRDFTerm(fm.puuid)} ;
          nie:dataSource            ${escapeRDFTerm(fm.vuri)} ;
          nfo:fileName              ${escapeRDFTerm(fm.pfilename)} ;
          dct:format                ${escapeRDFTerm(fm.pformat)} ;
          dbpedia:fileExtension     ${escapeRDFTerm(fm.pextension)} ;
          dct:created               ${escapeRDFTerm(fm.pcreated)} ;
          dct:modified              ${escapeRDFTerm(fm.pmodified)} .
      }
    }
  `;

  return updateSudo(queryString);
}

/*******************************************************************************
 * File download and storage
*******************************************************************************/

async function downloadAndSaveFile(vuuid) {

  //The options for the http request to the tunnel
  let options = {
    method:  "GET",
    host:    SYNC_BASE_URL.replace("http://", "").replace("/", ""),
    path:    DOWNLOAD_FILE_PATH.replace(":id", vuuid),
    port:    80,
  };

  console.log("Will download the file with uuid", vuuid);

  return new Promise((resolve, reject) => {

    //Setup the http request to the tunnel
    let req = http.request(options);

    req.on("response", (res) => {
      console.log("Inside the callback of the request.");
      //Open FileHandle and create WriteStream to it
      //FOR NODE 16 and up:
        //let writeFileHandle = await fs.open(`${puuid}.${ext}`, "wx");
        //let writeStream     = await writeFileHandle.createWriteStream();
      //FOR NODE 14:
      let writeStream = fs.createWriteStream(`${FILE_FOLDER}/${vuuid}`, { flags: "wx" });
      //You would want to use flags wx to prevent file from being overwritten
      //let writeStream = fs.createWriteStream(`${LOCAL_STORAGE_PATH}/${puuid}.${ext}`, { flags: "w" });
      console.log("Opened writeStream");
      //Close the file properly
      //FOR NODE 16 and up:
        //writeFileHandle.on("finish", () => writeFileHandle.close());
      //FOR NODE 14:
      writeStream.on("finish", () => {
        console.log("Closing the writeStream");
        writeStream.close();
      });
      writeStream.on("error", (err) => {
        if (err.errno == -17) {
          console.log("File already exists on local storage, ignoring");
          resolve();
        } else {
          reject(err);
        }
      });
      console.log("Placed a listener on finish");
      //On request finishing, pipe data directly to a file
      res.pipe(writeStream);
      console.log("Piped the streams");

      //res.on("data", (chunk) => {
      //  console.log("Recieved a chunk of data: ", chunk.toString());
      //});
      //

      res.on("error", (err) => {
        console.error("Error on the response for file download", err);
        console.log("Closing the writeStream");
        writeStream.close();
        reject(err);
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
      reject(err);
    });

    req.on("end", () => {
      resolve();
    });

    console.log("Placed all listeners in place");

    //Send the request
    req.end();
  });
}

function renameFile(oldname, newname) {
  return new Promise((resolve, reject) => {
    const oldpath = [FILE_FOLDER, oldname].join("/");
    const newpath = [FILE_FOLDER, newname].join("/");
    console.log(`Rename ${oldpath} into ${newpath}`);

    fs.rename(oldpath, newpath, (err) => {
      if (err) reject(err);
      resolve();
    });
  });
}

//Query for all the uuids of virtual files at once in the tempgraph
  //If file for that uuid not exists as physical file: download file and store to shared volume with name of vuuid
  //If file already exists: do nothing
//Request all metadata for the file at once
  //If all metadata present for that file:
    //rename file from its vuuid to filename of physical file from metadata
    //store all that data in final graph
  //If not all present leave in graph as is
