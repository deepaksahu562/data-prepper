/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import java.util.List;
import java.util.NavigableSet;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Implements the in memory buffer type.
 */
public class InMemoryBuffer implements BufferType {

    private S3Client s3Client;
    private S3SinkConfig s3SinkConfig;
    private Codec codec;

    public InMemoryBuffer() {
    }

    /**
     * @param s3Client
     * @param s3SinkConfig
     * @param codec
     */
    public InMemoryBuffer(final S3Client s3Client, final S3SinkConfig s3SinkConfig, final Codec codec) {
        this.s3Client = s3Client;
        this.s3SinkConfig = s3SinkConfig;
        this.codec = codec;
    }

    /**
     * @param events
     * @throws InterruptedException
     */
    public void inMemoryAccumulate(final List<Event> events) throws InterruptedException {
        bufferAccumulator(events, s3SinkConfig, codec);
    }

    public boolean flushRecordsToAmazonS3(List<String> records)  throws InterruptedException {
        StringBuilder eventBuilder = new StringBuilder();
        for (String record : records) {
            eventBuilder.append(record);
        }
        return uploadToAmazonS3(s3SinkConfig, s3Client, RequestBody.fromString(eventBuilder.toString()));
    }
}