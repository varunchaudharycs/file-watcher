# file-watcher
## Hadoop : 2.6
## Scala : 2.10.5
## Spark : 1.6.3
## SBT : 1.1.0
File watcher code in scala using spark. Uses WatchService of java.nio primarily.

Monitors a directory  and performs operations for file changes such as:
1. Create - Makes a trailer file to store hash value, number of lines and highest frequency word. Backup in HDFS.
2. Delete - Removes trailer file and moves corresponding files in HDFS (for later retrieval)
3. Modify - Performs corresponding actions of delete (for previous version) and create (for new version). Further changes in HDFS.

Additionally, a log file is maintained with timestamp, file and event name.

Maintains a hash map to store all hash values and a list of file names associated with the same.

On startup, it updates information of existing files in directory and creates missing trailer files. Also, starts hadoop services.

After termination of WatchService instance, it closes all open streams and instances.

Uses spark context to calculate number of lines and highest frequency word to increase speed and parallelisation.

Covers various corner cases such as:
1. File rename
2. Editing existing file
3. Duplicate file - Copies existing trailer file contents to reduce computation overhead
4. Processing large input of files
5. Processing large files
6. Empty files
7. Overflow condition
8. Error in WatchService instance(unavailability)

***To be continued ...***
