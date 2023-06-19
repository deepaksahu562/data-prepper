/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class MaxMindServiceConfigTest {

    @Test
    void getDatabasePathTest(){
        assertThat(new MaxMindServiceConfig().getDatabasePath(), equalTo(null));
    }

    @Test
    void getLoadTypeTest(){
        assertThat(new MaxMindServiceConfig().getLoadType(), equalTo(null));
    }

    @Test
    void getCacheSizeTest(){
        assertThat(new MaxMindServiceConfig().getCacheSize(), equalTo(null));
    }

    @Test
    void getCacheRefreshScheduleTest(){
        assertThat(new MaxMindServiceConfig().getCacheRefreshSchedule(), equalTo(null));
    }
}
