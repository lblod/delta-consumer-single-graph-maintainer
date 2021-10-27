## To Do

- Download and upload file
    > The process of downloading a file and putting it in the correct place.
    * [ ] Use uuid to download file
    * [ ] Save file with same name/uuid in same place
- Better triple filtering on files
    > Triples about files can be filtered based on their URI or on their predicates, but in most cases, file metadata is stored in a specific graph. We could use that graph as a means to filter triples.
    * [ ] Save graph information in delta files on consumer
    * [ ] Send graph information with delta files to consumer
    * [ ] In consumer, filter file metadata based on graph
    * [ ] Graph URI in config
- Triple flow
    > Listing the flow of triples.
    * [x] Data stored in different graphs in producer side
    * [x] Delta notifier sends all information to producer
    * [x] Producer stores triples in files, subfolder "deltas/physical-files"
    * [x] Producer file metadata in "json-diff-files" (see mu-authorization)
    * [x] Consumer downloads files from file service
    * [x] Consumer stores files in subfolder "consumer-files", no metadata
    * [x] Consumer imports data in single graph: "synced-files"

## Doing

- Intercept file triples
    > Find a good place for a filtering hook, store them, and process them at the right time.
    * [x] Find filtering hook place
    * [x] Store to separate graph
    * [x] Find mechanism for periodic scan of file metadata
    * [ ] Define minimum requirement to download file
    * [ ] Download and store file (see other task)
    * [ ] Only when all metadata arrived -> store metadata triples in main graph (not to flip out indexers in the stack)

## Done

- Understand workings
    > Play with creating triples and observe the flow of data
    * [x] Create a file to the file service
    * [x] Create triples about books and observe creation of sync files
    * [x] Observe synchronisation happening
