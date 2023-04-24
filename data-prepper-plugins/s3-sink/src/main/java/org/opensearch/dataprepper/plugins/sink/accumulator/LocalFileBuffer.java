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
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LocalFileBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private BufferedWriter bufferedWriter;
    private int eventCount;
    private final StopWatch watch;
    private File fileAbsolutePath;

    LocalFileBuffer() {
        // TODO: Get the size limitation and use that to avoid too much growth
        try {
            File file = new File(String.valueOf(UUID.randomUUID()));
            fileAbsolutePath = file.getAbsoluteFile();
            bufferedWriter = new BufferedWriter(new FileWriter(fileAbsolutePath));
        } catch (IOException e) {
            LOG.error("Unable to create temp file ", e);
        }

        eventCount = 0;

        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        return fileAbsolutePath.length();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public long getDuration(){
        return watch.getTime(TimeUnit.SECONDS);
    }
    @Override
    public void flushToS3(S3Client s3Client, String bucket, String key) {
        //final byte[] byteArray = byteArrayOutputStream.toByteArray();
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(fileAbsolutePath));

        removeTemporaryFile();
    }

    @Override
    public void writeEvent(byte[] bytes) throws IOException {
        //byteArrayOutputStream.write(bytes);
        bufferedWriter.write(Arrays.toString(bytes));
        eventCount++;
    }

    private void removeTemporaryFile() {
        if (fileAbsolutePath != null) {
            try {
                boolean isLocalFileDeleted = Files.deleteIfExists(Paths.get(fileAbsolutePath.toString()));
                if (isLocalFileDeleted) {
                    LOG.info("Local file deleted successfully {}", fileAbsolutePath);
                } else {
                    LOG.warn("Local file not deleted {}", fileAbsolutePath);
                }
            } catch (IOException e) {
                LOG.error("Local file unable to deleted {}", fileAbsolutePath, e);
            }
        }
    }
}
