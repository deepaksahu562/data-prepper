package com.amazon.situp.pipeline;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * PipelineConnector is a special type of Plugin which connects two pipelines acting both as Sink and Source.
 *
 * @param <T>
 */
public final class PipelineConnector<T extends Record<?>> implements Source<T>, Sink<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConnector.class);
    private static final int DEFAULT_WRITE_TIMEOUT = Integer.MAX_VALUE;
    private String sourcePipelineName; //name of the pipeline for which this connector acts as source
    private String sinkPipelineName; //name of the pipeline for which this connector acts as sink
    private Buffer<T> buffer;
    private AtomicBoolean isStopRequested;

    public PipelineConnector() {
        isStopRequested = new AtomicBoolean(false);
    }

    public PipelineConnector(final String sinkPipelineName) {
        this();
        this.sinkPipelineName = sinkPipelineName;
    }

    @Override
    public void start(final Buffer<T> buffer) {
        this.buffer = buffer;
    }

    @Override
    public void stop() {
        isStopRequested.set(true);
    }

    @Override
    public void output(final Collection<T> records) {
        if (buffer != null && !isStopRequested.get()) {
            for (T record : records) {
                try {
                    buffer.write(record, DEFAULT_WRITE_TIMEOUT);
                } catch (TimeoutException ex) {
                    LOG.error("PipelineConnector [{}-{}]: Timed out writing to pipeline [{}]",
                            sinkPipelineName, sourcePipelineName, sourcePipelineName, ex);
                    throw new RuntimeException(format("PipelineConnector [%s-%s]: Timed out writing to pipeline [%s]'s " +
                                    "buffer", sinkPipelineName, sourcePipelineName, sourcePipelineName), ex);
                }
            }
        } else {
            LOG.error("PipelineConnector [{}-{}]: Pipeline [{}] is currently not initialized or has been halted",
                    sinkPipelineName, sourcePipelineName, sourcePipelineName);
            throw new RuntimeException(format("PipelineConnector [%s-%s]: Pipeline [%s] is not active, " +
                    "cannot proceed", sinkPipelineName, sourcePipelineName, sourcePipelineName));
        }
    }

    public void setSourcePipelineName(final String sourcePipelineName) {
        this.sourcePipelineName = sourcePipelineName;
    }

    public void setSinkPipelineName(final String sinkPipelineName) {
        this.sinkPipelineName = sinkPipelineName;
    }
}
