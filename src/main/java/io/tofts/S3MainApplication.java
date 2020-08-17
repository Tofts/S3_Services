package io.tofts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3MainApplication {

    private static Logger logger = Logger.getLogger(S3MainApplication.class.getName());

    public static void main(String[] args) {
        BasicAWSCredentials creds = new BasicAWSCredentials("accessKey",
                "secretKey");

        AmazonS3Client s3 = new AmazonS3Client(creds);
        // S3 bucket operations
        // List buckets
        for (Bucket bucket : s3.listBuckets()) {
            logger.info(bucket.getName() + " " + bucket.getOwner() + " " + bucket.getCreationDate());

        }
        String newBucketName = "";

        // Create bucket
        try {
            newBucketName = "new-bucket-created-by-sdk" + System.currentTimeMillis();
            s3.createBucket(newBucketName);
            logger.info("Bucket Created");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }


        // File operations in S3 bucket
        final AmazonS3 awss3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        try {
            s3.putObject(newBucketName, "license.txt", new File("I:\\eclipse\\license.txt"));
        } catch (AmazonServiceException e) {
            logger.debug(e.getErrorMessage());
            System.exit(1);
        }

        // List files in bucket
        ListObjectsV2Result result = s3.listObjectsV2(newBucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            logger.info("* " + os.getKey());
        }

        // Download file from bcuekt
        try {
            S3Object o = s3.getObject(newBucketName, "file.txt");
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File("D:\\file.txt"));
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            logger.debug(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            logger.debug(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            logger.debug(e.getMessage());
            System.exit(1);
        }

        // Deleting  file from bucket
        try {
            s3.deleteObject(newBucketName, "file.txt");
            logger.info("file deleted");
        } catch (AmazonServiceException e1) {
            logger.debug(e1.getErrorMessage());
            System.exit(1);
        }

        // Delete multiple files from buceket
        logger.info("Delete multiple objects/files from bucket ");
        try {
            DeleteObjectsRequest dor = new DeleteObjectsRequest(newBucketName)
                    .withKeys("file.txt");
            s3.deleteObjects(dor);
        } catch (AmazonServiceException e2) {
            logger.debug(e2.getErrorMessage());
            System.exit(1);
        }

        // Delete a bucket
        try {
            s3.deleteBucket("new-bucket-created-by-sdk1592805368338");
            logger.info("Bucket Deleted");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }

    }
} 