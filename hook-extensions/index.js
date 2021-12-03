import singleHook from "./singleDispatch.js";

//// Example
//function dispatch(changeset) {
//  return new Promise((r, _) => r(changeset))
//    .then(mirror.dispatch)
//    .then(test1.dispatch)
//    .then(test2.dispatch);
//}

export function performHooks(changeset) {
  return singleHook(changeset);
}

