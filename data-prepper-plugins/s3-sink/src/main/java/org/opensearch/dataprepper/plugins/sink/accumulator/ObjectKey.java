package org.opensearch.dataprepper.plugins.sink.accumulator;

import java.util.regex.Pattern;

import org.opensearch.dataprepper.plugins.s3keyindex.S3ObjectIndex;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectKey {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectKey.class);
    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";
    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);

    private ObjectKey() {
    }

    /**
     * Building path inside bucket based on path_prefix.
     *
     * @param s3SinkConfig
     * @return s3ObjectPath
     */
    public static String buildingPathPrefix(final S3SinkConfig s3SinkConfig) {
        String pathPrefix = s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix();
        StringBuilder s3ObjectPath = new StringBuilder();
        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            String[] pathPrefixList = pathPrefix.split("\\/");
            for (int i = 0; i < pathPrefixList.length; i++) {
                if (SIMPLE_DURATION_PATTERN.matcher(pathPrefixList[i]).find()) {
                    s3ObjectPath.append(S3ObjectIndex.getObjectPathPrefix(pathPrefixList[i]) + "/");
                } else {
                    s3ObjectPath.append(pathPrefixList[i] + "/");
                }
            }
        }
        return s3ObjectPath.toString();
    }

    public static String objectFileName(S3SinkConfig s3SinkConfig) {
        return S3ObjectIndex
                .getObjectNameWithDateTimeId(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getNamePattern())
                + "." + codecFileExtension(s3SinkConfig);
    }

    public static String codecFileExtension(S3SinkConfig s3SinkConfig) {
        String codecFileExtension = s3SinkConfig.getCodec().getPluginName();
        if (codecFileExtension == null || codecFileExtension.isEmpty()) {
            codecFileExtension = DEFAULT_CODEC_FILE_EXTENSION;
        }
        return codecFileExtension;
    }
}