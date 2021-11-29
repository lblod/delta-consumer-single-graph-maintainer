import fs from 'fs-extra';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import fetch from 'node-fetch';
import path from 'path';
import {
    DELTA_FILE_FOLDER, DOWNLOAD_FILE_ENDPOINT, KEEP_DELTA_FILES
} from '../config';

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
  get filePath() {
    return path.join(DELTA_FILE_FOLDER,`${this.created}-${this.id}.json`);
  }

  async download() {
    try {
      await fetch(this.downloadUrl)
        .then(res => new Promise((resolve, reject) => {
          const writeStream = fs.createWriteStream(this.filePath);
          res.body.pipe(writeStream);
          writeStream.on('close', () => resolve());
          writeStream.on('error', reject);
      }));
    } catch(e) {
      console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
      console.log(e);
      throw e;
    }
  }

  async load() {
    try {
      await this.download();
      const changeSets = await fs.readJson(this.filePath, {encoding: 'utf-8'});

      const convertedChangeSets = [];
      for (let { inserts, deletes } of changeSets) {
        const changeSet = {};
        changeSet.deleteTermObjects = toTermObjectArray(deletes);
        changeSet.insertTermObjects = toTermObjectArray(inserts);
      }
      console.log(`Successfully loaded file ${this.id} stored at ${this.filePath}`);

      if(!KEEP_DELTA_FILES){
        await fs.unlink(this.filePath);
      }

      return convertedChangeSets;
    }
    catch(error){
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.filePath}`);
      console.log(error);
      throw error;
    }
  }
}

/**
 * Transform an array of triples to a string of statements to use in a SPARQL query
 *
 * @param {Array} triples Array of triples to convert
 * @method toTermObjectArray
 * @private
 */
function toTermObjectArray(triples) {
  const escape = function(rdfTerm) {
    const {type, value, datatype, 'xml:lang': lang} = rdfTerm;
    if (type === 'uri') {
      return sparqlEscapeUri(value);
    } else if (type === 'literal' || type === 'typed-literal') {
      if (datatype)
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype)}`;
      else if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    } else
      console.log(`Don't know how to escape type ${type}. Will escape as a string.`);
    return sparqlEscapeString(value);
  };

  return triples.map(function(t) {
    return {
      graph: escape(t.graph),
      subject: escape(t.subject),
      predicate: escape(t.predicate),
      object: escape(t.object)
    };
  });
}
