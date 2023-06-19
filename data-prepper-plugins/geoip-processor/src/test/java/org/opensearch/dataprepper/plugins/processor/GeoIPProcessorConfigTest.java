/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorConfigTest {

    @Test
    void getAwsAuthenticationOptionsTest(){
        assertThat(new GeoIPProcessorConfig().getAwsAuthenticationOptions(), equalTo(null));
    }

    @Test
    void getKeysConfigTest(){
        assertThat(new GeoIPProcessorConfig().getKeysConfig(), equalTo(null));
    }

    @Test
    void getServiceTypeTest(){
        assertThat(new GeoIPProcessorConfig().getServiceType(), equalTo(null));
    }

}
