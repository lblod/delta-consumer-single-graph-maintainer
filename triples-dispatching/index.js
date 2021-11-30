const initialSyncDispatching = tryLoadModule('/config/triples-dispatching/custom-dispatching/initial-sync-dispatching',
                                             './single-graph-dispatching/initial-sync-dispatching');
const deltaSyncDispatching = tryLoadModule('/config/triples-dispatching/custom-dispatching/delta-sync-dispatching',
                                           './single-graph-dispatching/delta-sync-dispatching');

function tryLoadModule(targetModulePath, fallbackModulePath){
  try {
    const module = require(targetModulePath);
    console.log(`Custom dispatching logic found on ${targetModulePath}`);
    return module;
  }
  catch(e) {
    if(e.code && e.code.toLowerCase() == 'MODULE_NOT_FOUND'.toLowerCase()){
      console.warn(`${targetModulePath} not found, assuming default behaviour loaded on ${fallbackModulePath}`);
      return require(fallbackModulePath);
    }
    else {
      console.error(`It seems something went wrong while loading dispatching-logic.`);
      console.error(`The provided parameters for custom module ${targetModulePath}. (Note: this is optional and can be empty`);
      console.error(`The provided parameters for default module ${fallbackModulePath}.`);
      throw e;
    }
  }
}

module.exports = {
  initialSyncDispatching,
  deltaSyncDispatching
};
