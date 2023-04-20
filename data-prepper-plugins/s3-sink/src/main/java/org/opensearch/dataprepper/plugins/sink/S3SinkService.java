/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Class responsible for taking an {@link S3SinkConfig} and creating all the necessary
 * {@link S3Client} and event accumulate operations.
 */
public class S3SinkService {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkService.class);
    private final S3SinkConfig s3SinkConfig;
    private final Lock reentrantLock;
    private final S3SinkWorker sinkWorker;

    /**
     * @param s3SinkConfig s3 sink related configuration.
     * @param codec parser
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig, final Codec codec) {
        this.s3SinkConfig = s3SinkConfig;
        reentrantLock = new ReentrantLock();
        sinkWorker = new S3SinkWorker(createS3Client(), s3SinkConfig, codec);
    }

    /**
     * @param events received buffered records add into queue.
     */
    public void accumulateBufferEvents(final List<Event> events) {
        reentrantLock.lock();
        try {
            sinkWorker.doAccumulator(events);
        } catch (Exception e) {
            LOG.error("Exception while accumulate buffer events: ", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @return {@link S3Client}
     */
    public S3Client createS3Client() {
        LOG.info("Creating S3 client");
        return S3Client.builder().region(s3SinkConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SinkConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(s3SinkConfig.getMaxConnectionRetries()).build())
                        .build())
                .build();
    }
}