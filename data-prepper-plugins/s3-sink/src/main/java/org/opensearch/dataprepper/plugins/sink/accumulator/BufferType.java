/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

/**
 * Interface for building buffer types.
 */
public interface BufferType {

    public static final Logger LOG = LoggerFactory.getLogger(BufferType.class);

    /**
     * Upload accumulated data to amazon s3 and perform retry in-case any issue occurred, based on
     * max_upload_retries configuration.
     * 
     * @param s3SinkConfig
     * @param s3Client
     * @param requestBody
     * @return
     * @throws InterruptedException
     */
    public default boolean uploadToAmazonS3(S3SinkConfig s3SinkConfig, S3Client s3Client, RequestBody requestBody)
            throws InterruptedException {

        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig);
        final String bucketName = s3SinkConfig.getBucketOptions().getBucketName();

        final String key = (pathPrefix != null && !pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
        boolean isFileUploadedToS3 = Boolean.FALSE;
        int retryCount = s3SinkConfig.getMaxUploadRetries();
        do {
            try {
                PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
                s3Client.putObject(request, requestBody);
                isFileUploadedToS3 = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Exception occurred while upload file {} to amazon s3 bucket. Retry count  : {} exception:",
                        namePattern, retryCount, e);
                --retryCount;
                if (retryCount == 0) {
                    return isFileUploadedToS3;
                }
                Thread.sleep(5000);
            }
        } while (!isFileUploadedToS3);
        return isFileUploadedToS3;
    }

    public abstract boolean flushRecordsToAmazonS3(List<String> records) throws InterruptedException ;

    public default void bufferAccumulator(final List<Event> events, S3SinkConfig s3SinkConfig, final Codec codec) {
        boolean isFileUploadedToS3 = Boolean.FALSE;
        int numEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        ByteCount byteCapacity = s3SinkConfig.getThresholdOptions().getMaximumSize();
        long duration = s3SinkConfig.getThresholdOptions().getEventCollect().getSeconds();

        List<String> records = new ArrayList<>();
        try {
            Iterator<Event> it = events.iterator();
            while (it.hasNext()) {
                int byteCount = 0;
                int eventCount = 0;
                long eventCollectionDuration = 0;
                StopWatch watch = new StopWatch();
                watch.start();
                int data = 0;
                while (thresholdsCheck(data, watch, byteCount, numEvents, byteCapacity, duration)) {
                    while (it.hasNext()) {
                        String jsonSerEvent = codec.parse(it.next());
                        byteCount += jsonSerEvent.getBytes().length;
                        records.add(jsonSerEvent.concat(System.lineSeparator()));
                        eventCount++;
                        data++;
                        eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
                    }
                }
                LOG.info(
                        "Snapshot info : Byte_capacity = {} Bytes, Event_count = {} Records & Event_collection_duration = {} Sec",
                        byteCount, eventCount, eventCollectionDuration);

                isFileUploadedToS3 = flushRecordsToAmazonS3(records);

                if (isFileUploadedToS3) {
                    LOG.info("Snapshot uploaded successfully");
                } else {
                    LOG.info("Snapshot upload failed");
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Exception while upload object to Amazon s3 bucket", e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.error("Exception while json serialization", e);
        }
    }

    private boolean thresholdsCheck(int eventCount, StopWatch watch, int byteCount, int numEvents, ByteCount byteCapacity, long duration) {
        if (eventCount > 0) {
            return eventCount < numEvents && watch.getTime(TimeUnit.SECONDS) < duration
                    && byteCount < byteCapacity.getBytes();
        } else {
            return watch.getTime(TimeUnit.SECONDS) < duration && byteCount < byteCapacity.getBytes();
        }
    }
}