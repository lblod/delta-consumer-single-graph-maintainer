## To Do

- Extra features
    > Some things to fix or make
    * [x] Make sure to test if a file exists explicitely to stop the downloading. Now, it trusts the failure of creating the write stream. Or try to create write stream before sending the request.
    * [x] Write JSDoc
- Needed improvements
    > These need to be done!
    * [ ] Sync-file ln73-ln79 needs to be a single query/transaction
- File remapping
    > Problem 2. Files can be put in different folders by different services. How to remap them to new folders?
    * [ ] Make test where a service puts a file in a subfolder
    * [ ] Make sure folder structure is remade like on the producer, look at the URI to recreate folders on the fly.
    * [ ] Filter on the folder structure, and be able to replace structure.

## Doing

- Download and upload file
    > The process of downloading a file and putting it in the correct place.
    * [x] Use uuid to download file
    * [x] Save file with same name/uuid in same place
    * [x] Allow for multiple attempts: add a triple with attempt number?
    * [x] When max attempts exceeded: do what exactly?
    * [ ] Revise the runFileSync function to be more concise, better at error handling and continuing execution on error. Make sure the process is divided in steps that always leave the database in a consistent state, and the application can crash between these steps.
- Tasks
    > Tasks to coördinate synchronisation of files
    * [ ] How do tasks work for the regulare file synchronisation?
    * [ ] How about initial sync?

## Done

- Understand workings
    > Play with creating triples and observe the flow of data
    * [x] Create a file to the file service
    * [x] Create triples about books and observe creation of sync files
    * [x] Observe synchronisation happening
- Intercept file triples
    > Find a good place for a filtering hook, store them, and process them at the right time.
    * [x] Find filtering hook place
    * [x] Store to separate temp graph
    * [x] Find mechanism for periodic scan of file metadata
    * [x] Define minimum requirement to download file
    * [x] Download and store file (see other task)
    * [x] Only when all metadata arrived -> store metadata triples in main graph (not to flip out indexers in the stack)
    * [x] Remove file triples from temp graph
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
- Error handling in file sync
    > Add error handling code during file synchronisation.
    * [x] Error messages per subtask
    * [x] Extend the previous error
- Removal of files
    > Files also need to be removed when metadata is removed.
    * [x] Find location when metadata is removed
    * [x] Write metadata to temp removal graph
    * [x] When minimum data received, remove physical file
    * [x] Store deletes to ingest graph
    * [x] Remove from temp removal graph
    * [x] Remove triples before files
