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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class InMemoryBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryBuffer.class);
    private final ByteArrayOutputStream byteArrayOutputStream;
    private int eventCount;
    private final StopWatch watch;

    InMemoryBuffer() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        eventCount = 0;

        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        return byteArrayOutputStream.size();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public long getDuration(){
        return watch.getTime(TimeUnit.SECONDS);
    }

    @Override
    public boolean flushToS3(S3Client s3Client, String bucket, String key) {
        boolean isFileUploadedToS3 = Boolean.FALSE;
        final byte[] byteArray = byteArrayOutputStream.toByteArray();
        try {
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromBytes(byteArray));
            isFileUploadedToS3 = Boolean.TRUE;
        }catch (Exception e){
            LOG.error("Exception while flush data to Amazon s3 bucket :", e);
        }
        return isFileUploadedToS3;
    }
    @Override
    public void writeEvent(byte[] bytes) throws IOException {
        byteArrayOutputStream.write(bytes);
        byteArrayOutputStream.write(System.lineSeparator().getBytes());
        eventCount++;
    }
}