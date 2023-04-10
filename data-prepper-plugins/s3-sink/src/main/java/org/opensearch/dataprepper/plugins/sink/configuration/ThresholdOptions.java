/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import java.time.Duration;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * An implementation class of s3 index configuration Options
 */
public class ThresholdOptions {

    private static final String DEFAULT_BYTE_CAPACITY = "50mb";

    @JsonProperty("event_count")
    @Size(min = 0, max = 10000000, message = "event_count size should be between 1 and 10000000")
    @NotNull
    private int eventCount;

    @JsonProperty("maximum_size")
    private String maximumSize = DEFAULT_BYTE_CAPACITY;

    @JsonProperty("event_collect")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 3600)
    @NotNull
    private Duration eventCollect;

    /**
     * Read event collection duration configuration
     */
    public Duration getEventCollect() {
        return eventCollect;
    }

    /**
     * Read byte capacity configuration
     */
    public ByteCount getMaximumSize() {
        return ByteCount.parse(maximumSize);
    }

    /**
     * Read the event count configuration
     */
    public int getEventCount() {
        return eventCount;
    }
}