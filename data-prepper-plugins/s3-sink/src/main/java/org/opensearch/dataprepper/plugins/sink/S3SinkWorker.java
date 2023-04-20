/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferType;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.LocalFileBuffer;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * Class responsible for threshold check and instantiation of {@link BufferType}
 */
public class S3SinkWorker {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkWorker.class);
    private final S3Client s3Client;
    private final S3SinkConfig s3SinkConfig;
    private final Codec codec;

    /**
     * @param s3Client
     * @param s3SinkConfig
     * @param codec
     */
    public S3SinkWorker(final S3Client s3Client, final S3SinkConfig s3SinkConfig, final Codec codec) {
        this.s3Client = s3Client;
        this.s3SinkConfig = s3SinkConfig;
        this.codec = codec;
    }

    /**
     * Accumulates data from buffer and store in local-file/in-memory.
     * @param events
     * @throws InterruptedException
     */
    public void doAccumulator(final List<Event> events) throws InterruptedException {
            if (s3SinkConfig.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
                new LocalFileBuffer(s3Client, s3SinkConfig, codec).localFileAccumulate(events);
            } else {
                new InMemoryBuffer(s3Client, s3SinkConfig, codec).inMemoryAccumulate(events);
            }
    }
}