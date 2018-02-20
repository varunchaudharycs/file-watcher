# file-watcher
### Hadoop : 2.6
### Scala : 2.10.5
### Spark : 1.6.3
### SBT : 1.1.0
File watcher code in scala using spark. Uses WatchService of java.nio primarily.

Monitors a directory  and performs operations for file changes such as:
1. CREATE - Makes a trailer file to store hash value, number of lines and highest frequency word. Backup in HDFS.
2. DELETE - Removes trailer file and moves corresponding files in HDFS (for later retrieval)
3. MODIFY - Deletes original trailer and creates a new one for the modified data.

Uses spark context to calculate number of lines and highest frequency word to increase speed.

Also:
1. Maintains LOG file with timestamp, file and event name
2. Maintains HASH MAP storing all hash values as 'keys' and trailer file fields of number of lines and highest frequency word as 'values'
3. On startup, it updates information of existing files in directory and creates missing trailer files.
4. Starts hadoop services.
5. After termination of WatchService instance, it closes all open streams and instances.

Covers various corner cases such as:
1. File rename
2. Editing existing file
3. Duplicate file - Copies existing trailer file contents to reduce computation overhead
4. Processing large input of files
5. Processing large files
6. Empty files - No trailer file created; message displayed
7. Error in WatchService instance(unavailability)
8. Unnecessary modify event thrown for a new 'Created' file
9. Unnreadable files - No trailer file created
9. 'Overflow' condition

***To be continued ...***
1. Using Kafka for handling events
2. Using spark-streaming on 'Consumer's' end 
3. Maintaining a hive table
4. Using oozie to schedule a job
