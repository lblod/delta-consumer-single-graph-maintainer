import { querySudo as query } from '@lblod/mu-auth-sudo';
import { exponentialGrowth, sleep } from './utils';

async function waitForDatabase(attempts = 0) {
  try {
    await query(`SELECT ?s WHERE { ?s ?p ?o } LIMIT 1`); // dummy query
  } catch (e) {
    attempts++;
    const timeout = exponentialGrowth(50, 0.3, attempts);
    console.log(`Database is not yet up, retrying in ${timeout}ms`);
    if (attempts > 35)
      console.log(e);
    await sleep(timeout);
    await waitForDatabase(attempts);
  }
}

export {
  waitForDatabase
};
