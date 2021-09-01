package com.mongodb.performance.gridfs;


import java.io.*;
import java.nio.file.Files;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerUploadStream implements Runnable {

    Logger logger;
    MongoDatabase gridFSDB;
    GridFSBucket gridFSBucket;
    int threadId;

    WorkerUploadStream(int threadId, MongoDatabase gridFSDB, GridFSBucket gridFSBucket) {
        this.gridFSDB = gridFSDB;
        this.gridFSBucket=gridFSBucket;
        logger = LoggerFactory.getLogger(WorkerUpload.class);
        this.threadId = threadId;
    }
    public void run() {

        /*
        * The GridFSUploadStream buffers data until it reaches the chunkSizeBytes and then inserts
        * the chunk into the chunks collection. When the GridFSUploadStream is closed, the final chunk
        * is written and the file metadata is inserted into the files collection.
        * */
        try {
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(1048576)
                    .metadata(new Document("type", "test-"+ threadId));
            GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("mongodb-test-"+threadId, options);
            byte[] data = Files.readAllBytes(new File("/Users/ilian/Downloads/test.zip").toPath());

            uploadStream.write(data);
            uploadStream.close();
          logger.info("The fileId of the uploaded file is:[" + uploadStream.getObjectId().toHexString() + "] document creation date:["+uploadStream.getObjectId().getDate()+"]");


        } catch (IOException e){
            e.printStackTrace();
        }
    }

}