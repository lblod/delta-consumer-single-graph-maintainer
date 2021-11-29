import fs from 'fs-extra';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { Parser } from 'n3';
import fetch from 'node-fetch';
import path from 'path';
import {
    DOWNLOAD_FILE_ENDPOINT,
    DUMPFILE_FOLDER, SYNC_BASE_URL,
    SYNC_DATASET_ENDPOINT, SYNC_DATASET_SUBJECT, SYNC_FILES_PATH
} from '../config';

const BASEPATH = path.join(SYNC_FILES_PATH, DUMPFILE_FOLDER);
fs.ensureDirSync(BASEPATH);

class DumpFile {
  constructor(distributionData, data) {
    this.id = data.id;
    this.created = distributionData["release-date"];
    this.name = distributionData["title"];
  }

  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  get filePath() {
    return path.join(BASEPATH,`${this.id}.ttl`);
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
      const data = fs.readFileSync(this.filePath, 'utf8');
      const triples = await this.toTermObjectArray(data);
      console.log(`Successfully loaded file ${this.id} stored at ${this.filePath}`);
      return triples;
    }
    catch(error){
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.filePath}`);
      console.log(error);
      throw error;
    }
  }

  serializeN3Term(rdfTerm) {
    //Based on: https://github.com/kanselarij-vlaanderen/dcat-dataset-publication-service/blob/master/lib/ttl-helpers.js#L48
    const {termType, value, datatype, language} = rdfTerm;
    if (termType === 'NamedNode') {
      return sparqlEscapeUri(value);
    }
    else if (termType === 'Literal') {
      // We ignore xsd:string datatypes because Virtuoso doesn't treat those as default datatype
      // Eg. SELECT * WHERE { ?s mu:uuid "4983948" } will not return any value if the uuid is a typed literal
      // Since the n3 npm library used by the producer explicitely adds xsd:string on non-typed literals
      // we ignore the xsd:string on ingest
      if (datatype && datatype.value && datatype.value != 'http://www.w3.org/2001/XMLSchema#string')
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype.value)}`;
      else if (language)
        return `${sparqlEscapeString(value)}@${language}`;
      else
        return `${sparqlEscapeString(value)}`;
    }
    else {
      console.log(`Don't know how to escape type ${termType}. Will escape as a string.`);
      return sparqlEscapeString(value);
    }
  }

  //Helper function to parse TTL file.
  async toTermObjectArray(data) {
    const parser = new Parser();
    const quads = parser.parse(data);
    const triples = [];
    for(const quad of quads){
      triples.push(
        {
          graph: null,
          subject: this.serializeN3Term(quad.subject),
          predicate: this.serializeN3Term(quad.predicate),
          object: this.serializeN3Term(quad.object)
        }
      );
    }
    return triples;
  }
}

export async function getLatestDumpFile() {
  try {
    console.log(`Retrieving latest dataset from ${SYNC_DATASET_ENDPOINT}`);
    const responseDataset = await fetch(
      `${SYNC_DATASET_ENDPOINT}?filter[subject]=${SYNC_DATASET_SUBJECT}&filter[:has-no:next-version]=yes`,
      {
        headers: {
          'Accept': 'application/vnd.api+json'
        }
      }
    );
    const dataset = await responseDataset.json();

    if (dataset.data.length) {
      const distributionMetaData = dataset.data[0].attributes;
      const distributionRelatedLink = dataset.data[0].relationships.distributions.links.related;
      const distributionUri = `${SYNC_BASE_URL}/${distributionRelatedLink}`;

      console.log(`Retrieving distribution from ${distributionUri}`);
      const resultDistribution = await fetch(`${distributionUri}?include=subject`, {
        headers: {
          'Accept': 'application/vnd.api+json'
        }
      });
      const distribution = await resultDistribution.json();
      return new DumpFile(distributionMetaData, distribution.data[0].relationships.subject.data);
    } else {
      throw 'No dataset was found at the producing endpoint.';
    }
  } catch (e) {
    console.log(`Unable to retrieve dataset from ${SYNC_DATASET_ENDPOINT}`);
    throw e;
  }
}
