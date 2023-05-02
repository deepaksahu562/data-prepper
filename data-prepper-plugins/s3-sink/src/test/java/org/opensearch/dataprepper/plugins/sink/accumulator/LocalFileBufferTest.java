package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ExtendWith(MockitoExtension.class)
class LocalFileBufferTest {

    @Mock
    private S3Client s3Client;
    private LocalFileBuffer localFileBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        localFileBuffer = new LocalFileBuffer();

        while (localFileBuffer.getEventCount() < 55) {
            localFileBuffer.writeEvent(generateByteArray());
        }
        assertThat(localFileBuffer.getSize(), greaterThan(1l));
        assertThat(localFileBuffer.getEventCount(), equalTo(55));
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(0L));
    }

    @Test
    void test_without_write_event_into_buffer() {
        localFileBuffer = new LocalFileBuffer();
        assertThat(localFileBuffer.getSize(), equalTo(0L));
        assertThat(localFileBuffer.getEventCount(), equalTo(0));
        assertThat(localFileBuffer.getDuration(), lessThanOrEqualTo(0L));

    }

    @Test
    void test_with_write_event_into_buffer_and_flush_toS3() throws IOException {
        localFileBuffer = new LocalFileBuffer();

        while (localFileBuffer.getEventCount() < 55) {
            localFileBuffer.writeEvent(generateByteArray());
        }
        assertThat(localFileBuffer.getSize(), greaterThan(1l));
        assertThat(localFileBuffer.getEventCount(), equalTo(55));
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(0L));

        boolean isUploadedToS3 = localFileBuffer.flushToS3(s3Client, "data-prepper", "log.txt");
        Assertions.assertTrue(isUploadedToS3);
    }

    @Test
    void test_uploadedToS3_success() {
        localFileBuffer = new LocalFileBuffer();
        Assertions.assertNotNull(localFileBuffer);
        boolean isUploadedToS3 = localFileBuffer.flushToS3(s3Client, "data-prepper", "log.txt");
        Assertions.assertTrue(isUploadedToS3);
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}