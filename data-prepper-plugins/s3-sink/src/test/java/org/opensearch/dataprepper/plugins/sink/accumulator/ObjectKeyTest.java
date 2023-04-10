package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;

@ExtendWith(MockitoExtension.class)
class ObjectKeyTest {

    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";
    @Mock
    private ObjectKey objectKey;
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private PluginModel pluginModel;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private JsonCodec codec;
    @Mock
    private BucketOptions bucketOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    void test_buildingPathPrefix(){
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(objectKeyOptions.getPathPrefix()).thenReturn("events/%{yyyy}/%{MM}/%{dd}/");

        String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        assertNotNull(pathPrefix);
        assertThat(pathPrefix, startsWith("events"));
    }

    @Test
    void test_objectFileName(){
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(DEFAULT_CODEC_FILE_EXTENSION);
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        String objectFileName = ObjectKey.objectFileName(s3SinkConfig);
        assertNotNull(objectFileName);
        assertThat(objectFileName, startsWith("my-elb"));
    }

    @Test
    void test_codecFileExtension_invoke_inner_functions_in_order() {

        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(DEFAULT_CODEC_FILE_EXTENSION);

        String codecFileExtension = ObjectKey.codecFileExtension(s3SinkConfig);
        assertNotNull(codecFileExtension);
        assertThat(codecFileExtension, equalTo(DEFAULT_CODEC_FILE_EXTENSION));

        InOrder inOrder = inOrder(s3SinkConfig, pluginModel);
        inOrder.verify(s3SinkConfig).getCodec();
        inOrder.verify(pluginModel).getPluginName();

        verifyNoMoreInteractions(s3SinkConfig);
        verifyNoMoreInteractions(pluginModel);
    }

    @Test
    void test_codecFileExtension() {

        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(s3SinkConfig.getCodec().getPluginName()).thenReturn("ndjson");

        String codecFileExtension = ObjectKey.codecFileExtension(s3SinkConfig);
        assertNotNull(codecFileExtension);
        assertThat(codecFileExtension, equalTo("ndjson"));
    }

    @Test
    void test_default_codecFileExtension() {

        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(null);

        String codecFileExtension = ObjectKey.codecFileExtension(s3SinkConfig);
        assertNotNull(codecFileExtension);
        assertThat(codecFileExtension, equalTo(DEFAULT_CODEC_FILE_EXTENSION));

    }

}