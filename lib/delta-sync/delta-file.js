import fs from 'fs-extra';
import path from 'path';
import fetch from 'node-fetch';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE,
  INGEST_GRAPH,
  KEEP_DELTA_FILES,
  DELTA_FILE_FOLDER,
  UPLOADED_FILES_GRAPH,
  TEMP_FILE_GRAPH,
  TEMP_FILE_REMOVAL_GRAPH,
  FILE_PREFIXES,
  MU_CALL_SCOPE_ID_FILE_SYNC,
  PERFORM_SEPERATE_FILE_SYNC
} from '../../config';
import { partition, toStatements, tripleToMetaTriples } from "../utils.js";

fs.ensureDirSync(DELTA_FILE_FOLDER);

export default class DeltaFile {
  constructor(data) {
    /** Id of the delta file */
    this.id = data.id;
    /** Creation datetime of the delta file */
    this.created = data.attributes.created;
    /** Name of the delta file */
    this.name = data.attributes.name;
  }

  /**
   * Public endpoint to download the delta file from based on its id
   */
  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  /**
   * Location to store the delta file during processing
   */
  get deltaFilePath() {
    return path.join(DELTA_FILE_FOLDER,`${this.created}-${this.id}.json`);
  }

  /**
   * Trigger consumption of a delta file.
   * I.e. processing the insert/delete changesets and applying the changes
   * in the triple store taking into account the authorization rules.
   *
   * @param {function} onFinishCallback Callback executed when the processing of the delta file finished,
   *                         either successfully or unsuccessfully.
   *                         The callback function receives 2 arguments:
   *                         - the delta file object
   *                         - a boolean indicating success (true) or failure (false)
   * @method consume
   * @public
   */
  async consume(onFinishCallback) {
    const writeStream = fs.createWriteStream(this.deltaFilePath);
    writeStream.on('finish', () => this.ingest(onFinishCallback));
    console.log(`Wrote deltas to file ${this.deltaFilePath}`);

    try {
      const response = await fetch(this.downloadUrl);
      response.body.pipe(writeStream);
    } catch(e) {
      console.log(`Something went wrong while consuming the file ${this.id} from ${this.downloadUrl}`);
      console.log(e);
      await onFinishCallback(this, false);
      throw e;
    }
  }

  /**
   * Process the insert/delete changesets and apply the changes
   * in the triple store taking into account the authorization rules.
   *
   * @param {function} onFinishCallback Callback executed when the processing of the delta file finished,
   *                         either successfully or unsuccessfully.
   *                         The callback function receives 2 arguments:
   *                         - the delta file object
   *                         - a boolean indicating success (true) or failure (false)
   * @method ingest
   * @private
   */
  async ingest(onFinishCallback) {
    console.log(`Start ingesting file ${this.id} stored at ${this.deltaFilePath}`);
    try {
      const changeSets = await fs.readJson(this.deltaFilePath, {encoding: 'utf-8'});
      console.log("WHILE INGESTING, this is a changeSets: ", JSON.stringify(changeSets));
      for (let {inserts, deletes} of changeSets) {
        //Filter on file triples, based on URI prefix
        const partitionedInserts = partition(inserts, change => FILE_PREFIXES.some(pref => change.subject.value.startsWith(pref)));
        const partitionedDeletes = partition(deletes, change => FILE_PREFIXES.some(pref => change.subject.value.startsWith(pref)));
        const regularInserts = partitionedInserts.fails;
        const regularDeletes = partitionedDeletes.fails;
        const fileInserts = partitionedInserts.passes;
        const fileDeletes = partitionedDeletes.passes;

        //Store regular triples in the ingest graph
        console.log(`Inserting data in graph <${INGEST_GRAPH}>`);
        await processInserts(regularInserts, INGEST_GRAPH);
        console.log("Deleting data in all graphs");
        await processDeletes(regularDeletes);

        //Store triples about files in a temporary graph, will process later, only if wanted. Otherwise insert in ingest graph
        if (PERFORM_SEPERATE_FILE_SYNC) {
          //Convert triples into metadata, because of the buffering for files
          const fileInsertsMeta = fileInserts.flatMap(tripleToMetaTriples);
          const fileDeletesMeta = fileDeletes.flatMap(tripleToMetaTriples);

          await processInserts(fileInsertsMeta, TEMP_FILE_GRAPH, MU_CALL_SCOPE_ID_FILE_SYNC);
          await processInserts(fileDeletesMeta, TEMP_FILE_REMOVAL_GRAPH, MU_CALL_SCOPE_ID_FILE_SYNC);
        }
        else {
          await processInserts(fileInserts, INGEST_GRAPH);
          await processInserts(fileDeletes, INGEST_GRAPH);
        }
      }
      console.log(`Successfully finished ingesting file ${this.id} stored at ${this.deltaFilePath}`);
      await onFinishCallback(this, true);

      if(!KEEP_DELTA_FILES){
        await fs.unlink(this.deltaFilePath);
      }
    } catch (e) {
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.deltaFilePath}`);
      console.log(e);
      await onFinishCallback(this, false);
      throw e;
    }
  }
}

async function processInserts(triples, graph, scope) {
  //TODO dispatching in the future
  return insertTriplesInGraph(triples, graph, scope);
}

async function processDeletes(triples, graph) {
  //TODO dispatching in the future
  return deleteTriplesInAllGraphs(triples);
}

/**
 * Insert the list of triples in a defined graph in the store
 *
 * @param triples {Array} Array of triples from an insert changeset
 * @param graph {string} Graph to insert the triples into
 * @param {String} scope - Scope identifier for filtering in the deltanotifier.
 * @method insertTriplesInGraph
 * @private
 */
async function insertTriplesInGraph(triples, graph, scope) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting ${triples.length} triples in batches. Current batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    const scopeID = (scope ? { 'mu-call-scope-id': scope } : undefined);
    await update(`
      INSERT DATA {
          GRAPH <${graph}> {
              ${statements}
          }
      }
    `, scopeID);
  }
}

/**
 * Delete the triples from the given list from all graphs in the store, including the temporary graph.
 * Note: Triples are deleted one by one to avoid the need to use OPTIONAL in the WHERE clause
 *
 * @param {Array} triples Array of triples from an insert changeset
 * @method insertTriplesInTmpGraph
 * @private
 */
async function deleteTriplesInAllGraphs(triples) {
  console.log(`Deleting ${triples.length} triples one by one in all graphs`);
  for (let i = 0; i < triples.length; i++) {
    const statements = toStatements([triples[i]]);
    await update(`
      DELETE WHERE {
          GRAPH ?g {
              ${statements}
          }
      }
    `);
  }
}

