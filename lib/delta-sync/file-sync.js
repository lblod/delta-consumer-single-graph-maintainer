import { query, update, sparqlEscapeUri, sparqlEscapeString } from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { escapeRDFTerm, storeError } from "../utils.js";
import StackableError from "../../StackableError.js";
import http from "http";
import https from "https";
import fs   from "fs";
import path from "path";
import url  from "url";
import {
  DISABLE_FILE_INGEST,
  PREFIXES,
  TEMP_FILE_GRAPH,
  SYNC_BASE_URL,
  DOWNLOAD_FILE_PATH,
  FILE_FOLDER,
  INGEST_GRAPH,
  TEMP_FILE_REMOVAL_GRAPH,
  REMAPPING,
  MU_CALL_SCOPE_ID_FILE_SYNC
} from '../../config.js';

//Configure whether we need http or https
const http_s = ((new url.URL(SYNC_BASE_URL)).protocol === "http:" ? http : https);

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
    //Store the error to the database, also include the stacktrace for better debugging (even though the stack trace might be useless when the code changes over time).
    await storeError(err.toString());
  }
}

async function runFileSync() {
}

/*******************************************************************************
 * Subtask functions
*******************************************************************************/


/*******************************************************************************
 * Database access
*******************************************************************************/


/*******************************************************************************
 * File download and storage
*******************************************************************************/

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
function downloadAndSaveFile(vuuid, filepath) {

  if (!filepath.startsWith(FILE_FOLDER)) {
    throw new StackableError(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}. Files can get lost like this!`);
  }

  //The options for the http request to the producer
  const filehosturl = new url.URL(SYNC_BASE_URL);
  let options = {
    method:   "GET",
    host:     filehosturl.hostname,
    path:     DOWNLOAD_FILE_PATH.replace(":id", vuuid),
    protocol: filehosturl.protocol
  };

  console.log("Will download the file with uuid", vuuid);

  return new Promise((resolve, reject) => {

    //First make sure the destination path exists
    let targetdir = path.dirname(filepath);
    fs.mkdir(targetdir, { recursive: true }, err => {
      if (err) {
        reject(new StackableError(`Creating target directory ${targetdir} for file download failed.`));
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
        reject(new StackableError(`Error while downloading and saving file ${vuuid} on the consumer, during writing to the filestream.`, err));
      }
    });
    writeStream.on("ready", async () => {
      resolve(writeStream);
    });
  })).then((writeStream) => new Promise(async (resolve, reject) => {
    let req;
    try {
      //Setup the http request
      req = http_s.request(options);
    }
    catch (err) {
      writeStream.close();
      await deleteFile(filepath);
      reject(new StackableError("Starting of request failed", err));
    }

    req.on("response", (res) => {
      console.log("Inside the callback of the request.");
      //On request finishing, pipe data directly to a file
      res.pipe(writeStream);
      console.log("Piped the streams");

      res.on("error", async (err) => {
        console.error("Error on the response for file download", err);
        console.log("Closing the writeStream");
        writeStream.close();
        await deleteFile(filepath);
        reject(new StackableError(`Error while downloading and saving file ${vuuid} on the consumer, during the request of the file to the producer file service.`, err));
      });

      res.on("end", () => {
        console.log("End of the request reached with statuscode: ", res.statusCode);
        console.log("Closing the writeStream");
        writeStream.close();
        resolve();
      });
    });

    req.on("timeout", async (err) => {
      console.error(err);
      req.destroy();
      await deleteFile(filepath);
      reject(new StackableError(`Error while downloading and saving file ${vuuid} on the consumer, due to a timeout on the request to the producer file service.`, err));
    });

    req.on("end", () => {
      resolve();
    });

    //Send the request
    req.end();
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

