package com.mongodb.performance.gridfs;


import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

public class WorkerUpload implements Runnable {

    Logger logger;
    MongoDatabase gridFSDB;
    GridFSBucket gridFSBucket;
    int threadId;

    WorkerUpload(int threadId, MongoDatabase gridFSDB, GridFSBucket gridFSBucket) {
        this.gridFSDB = gridFSDB;
        this.gridFSBucket=gridFSBucket;
        logger = LoggerFactory.getLogger(WorkerUpload.class);
        this.threadId = threadId;
    }

    public void run() {
        try {

            InputStream streamToUploadFrom = new FileInputStream(new File("/Users/ilian/Downloads/test.zip"));

            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(1048576)
                    .metadata(new Document("type", "test-"+threadId));
            ObjectId fileId = gridFSBucket.uploadFromStream("mongodb-test"+threadId, streamToUploadFrom, options);
            logger.info("The fileId of the uploaded file is:[" + fileId.toHexString() + "] document creation date:[" + fileId.getDate() + "]");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}