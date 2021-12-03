//This is an example implementation of how to transform changesets
//This example changes the graph to a single fixed URI for all triples. Do more complex filtering on the subject of the triple, the predicate, or the object to figure out to what graph this triple needs to be inserted.
//Can also be used to filter triples, by not including them in the results.
export default function singledispatch(changeset) {
  let { inserts, deletes } = changeset;
  inserts.forEach(i => i.graph.value = "http://mu.semte.ch/graphs/files");
  return { inserts, deletes };
}

