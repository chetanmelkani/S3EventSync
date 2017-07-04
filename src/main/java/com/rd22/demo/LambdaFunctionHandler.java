package com.rd22.demo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;

import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {
	
    private static final Logger logger = Logger.getLogger(LambdaFunctionHandler.class);

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            logger.debug("srcBucket: " + srcBucket);
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            logger.debug("srcKey: " + srcKey);
            
            String dstBucket = "chemel-destination";
            String dstKey = srcKey + "-copy";

            // Sanity check: validate that source and destination are different
            // buckets.
            if (srcBucket.equals(dstBucket)) {
            	logger.debug("Destination bucket must not match source bucket.");
                return "";
            }

            // Infer the image type.

            // Download the image from S3 into a stream
            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            InputStream objectData = s3Object.getObjectContent();

            logger.debug("Writing to: " + dstBucket + "/" + dstKey);
            s3Client.copyObject(srcBucket, srcKey, dstBucket, dstKey);
            // Uploading to S3 destination bucket
            logger.debug("returning ok");
            return "Ok";
        } catch (IOException e) {
        	logger.error("IOException", e);
            throw new RuntimeException(e);
        }
        catch(Exception e){
        	logger.error("Exception", e);
        	return "exception";
        }
    }

}
