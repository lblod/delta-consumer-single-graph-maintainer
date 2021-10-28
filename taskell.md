## To Do


## Doing

- Extra features
    > Some things to fix or make
    * [x] Make sure to test if a file exists explicitely to stop the downloading. Now, it trusts the failure of creating the write stream. Or try to create write stream before sending the request.
    * [ ] Use tasks for the sync of files

## Done

- Understand workings
    > Play with creating triples and observe the flow of data
    * [x] Create a file to the file service
    * [x] Create triples about books and observe creation of sync files
    * [x] Observe synchronisation happening
- Download and upload file
    > The process of downloading a file and putting it in the correct place.
    * [x] Use uuid to download file
    * [x] Save file with same name/uuid in same place
- Intercept file triples
    > Find a good place for a filtering hook, store them, and process them at the right time.
    * [x] Find filtering hook place
    * [x] Store to separate temp graph
    * [x] Find mechanism for periodic scan of file metadata
    * [x] Define minimum requirement to download file
    * [x] Download and store file (see other task)
    * [x] Only when all metadata arrived -> store metadata triples in main graph (not to flip out indexers in the stack)
    * [x] Remove file triples from temp graph
- Removal of files
    > Files also need to be removed when metadata is removed.
    * [x] Find location when metadata is removed
    * [x] Write metadata to temp removal graph
    * [x] When minimum data received, remove physical file
    * [x] Store deletes to ingest graph
    * [x] Remove from temp removal graph
- Better triple filtering on files
    > Triples about files can be filtered based on their URI or on their predicates, but in most cases, file metadata is stored in a specific graph. We could use that graph as a means to filter triples.
    * [x] CHECK graph information in delta files on consumer
    * [x] CHECK  graph information with delta files sent to consumer
    * [x] In consumer, filter file metadata based on graph
    * [x] Graph URI in config
- Triple flow
    > Listing the basic flow of triples.
    * [x] Data stored in different graphs in producer side
    * [x] Delta notifier sends all information to producer
    * [x] Producer stores triples in files, subfolder "deltas/physical-files"
    * [x] Producer file metadata in "json-diff-files" (see mu-authorization)
    * [x] Consumer downloads files from file service
    * [x] Consumer stores files in subfolder "consumer-files", no metadata
    * [x] Consumer imports data in single graph: "synced-files"
