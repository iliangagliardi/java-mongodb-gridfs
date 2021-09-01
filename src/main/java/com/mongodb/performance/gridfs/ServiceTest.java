package com.mongodb.performance.gridfs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import org.apache.commons.lang.time.StopWatch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class ServiceTest {
    static final String version = "0.0.1";
    static Logger logger;
    List<String> controlList = new ArrayList<String>();



    public static void main(String[] args) {
        LogManager.getLogManager().reset();
        MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://node1:27017,node2:27017,node3:27017/?replicaSet=rs&connectTimeoutMS=5000&socketTimeoutMS=60000&compressors=snappy&maxPoolSize=200&minPoolSize=50"))
                .credential(MongoCredential.createCredential("ilian", "admin", "Password.".toCharArray()))
                .build());
        MongoDatabase gridFSDB = mongoClient.getDatabase("gridFSDB");
        GridFSBucket gridFSBucket = GridFSBuckets.create(gridFSDB,"gridFS")
                .withWriteConcern(WriteConcern.MAJORITY);


        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        logger = LoggerFactory.getLogger(ServiceTest.class);
        logger.info(version);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int nThreads = 50;

        ExecutorService simExec = Executors.newFixedThreadPool(nThreads);

        for (int i = 0; i < nThreads; i++) {
            simExec.execute(new WorkerUploadStream(i, gridFSDB, gridFSBucket));
        }

        simExec.shutdown();

        try {
            simExec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            simExec.shutdown();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());

        }

        stopWatch.stop();
        logger.info("# Execution Time: " + stopWatch.getTime() + " millis");
        stopWatch = null;




    }

}
