/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.accumulator.*;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;

/**
 * Implementation class of s3-sink plugin. It is responsible for receive the collection of
 * {@link Event} and upload to amazon s3 based on thresholds configured.
 */
@DataPrepperPlugin(name = "s3", pluginType = Sink.class, pluginConfigurationType = S3SinkConfig.class)
public class S3Sink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);
    private final S3SinkConfig s3SinkConfig;
    private final Codec codec;
    private volatile boolean sinkInitialized;
    private S3SinkService s3SinkService;
    private final BufferFactory bufferFactory;

    /**
     * @param pluginSetting dp plugin settings.
     * @param s3SinkConfig s3 sink configurations.
     * @param pluginFactory dp plugin factory.
     */
    @DataPrepperPluginConstructor
    public S3Sink(final PluginSetting pluginSetting, final S3SinkConfig s3SinkConfig,
                  final PluginFactory pluginFactory) {
        super(pluginSetting);
        this.s3SinkConfig = s3SinkConfig;
        final PluginModel codecConfiguration = s3SinkConfig.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;

        if (s3SinkConfig.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            bufferFactory = new LocalFileBufferFactory();
        } else {
            bufferFactory = new InMemoryBufferFactory();
        }
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize s3-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize s3-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    /**
     * Initialize {@link S3SinkService}
     */
    private void doInitializeInternal() {
        s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec);
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }
        s3SinkService.output(records);
    }
}