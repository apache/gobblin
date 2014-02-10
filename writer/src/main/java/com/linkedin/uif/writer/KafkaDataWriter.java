package com.linkedin.uif.writer;

import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.converter.DataConverter;

import java.io.IOException;

/**
 * An implementation of {@link DataWriter} that writes to a Kafka topic.
 *
 * @param <D> type of source data record representation
 */
class KafkaDataWriter<D> implements DataWriter<D> {

    @Override
    public void write(D sourceRecord) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void commit() throws IOException {
    }

    @Override
    public void cleanup() throws IOException {
    }

    @Override
    public long recordsWritten() {
        return 0;
    }
}
