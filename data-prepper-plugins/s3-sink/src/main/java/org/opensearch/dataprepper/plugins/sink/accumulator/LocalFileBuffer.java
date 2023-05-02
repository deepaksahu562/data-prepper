/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold local file data and flushing it to S3.
 */
public class LocalFileBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private BufferedOutputStream bufferedOutputStream;
    private int eventCount;
    private final StopWatch watch;
    private File localFile;

    LocalFileBuffer() {
        try {
            localFile = new File(String.valueOf(UUID.randomUUID()));
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(localFile));
            LOG.info("Local file created - {}", localFile);
        } catch (IOException e) {
            LOG.error("Unable to create local file ", e);
        }

        eventCount = 0;

        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        return localFile.length();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public long getDuration(){
        return watch.getTime(TimeUnit.SECONDS);
    }

    /**
     * Upload accumulated data to amazon s3
     * @param s3Client s3 client object.
     * @param bucket bucket name.
     * @param key s3 object key path.
     * @return boolean based on file upload status.
     */
    @Override
    public boolean flushToS3(S3Client s3Client, String bucket, String key) {
        boolean isFileUploadedToS3 = Boolean.FALSE;
        try {
            bufferedOutputStream.flush();
            bufferedOutputStream.close();
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromFile(localFile));
            removeTemporaryFile();
            isFileUploadedToS3 = Boolean.TRUE;
        } catch (Exception e) {
            LOG.error("Exception while flush data to Amazon s3 bucket :", e);
        }
        return isFileUploadedToS3;
    }

    /**
     * write byte array to output stream.
     * @param bytes byte array.
     * @throws IOException while writing to output stream fails.
     */
    @Override
    public void writeEvent(byte[] bytes) throws IOException {
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.write(System.lineSeparator().getBytes());
        eventCount++;
    }

    /**
     * Remove the local temp file after flushing data to s3.
     */
    private void removeTemporaryFile() {
        if (localFile != null) {
            try {
                boolean isLocalFileDeleted = Files.deleteIfExists(Paths.get(localFile.toString()));
                if (isLocalFileDeleted) {
                    LOG.info("Local file has been deleted successfully {}", localFile);
                } else {
                    LOG.warn("Local file has not been deleted {}", localFile);
                }
            } catch (IOException e) {
                LOG.error("Local file unable to deleted {}", localFile, e);
            }
        }
    }
}