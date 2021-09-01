# Test GridFS
build with: mvn package 
run with: Java -jar TestGridFS.jar 
or 
mvn compile exec:java -Dexec.mainClass="com.mongodb.performance.gridfs.GridFSTest"
log in: TestGridFS.log

change the code to use WorkerUpload or WorkerUploadStream. The second one uses GridFSUploadStream
The GridFSUploadStream buffers data until it reaches the chunkSizeBytes and then inserts the chunk into the chunks collection. When the GridFSUploadStream is closed, the final chunk is written and the file metadata is inserted into the files collection.
