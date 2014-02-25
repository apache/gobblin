package com.linkedin.uif.writer;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * An implementation of {@link DataWriter} that writes to a Kafka topic.
 *
 * @param <S> type of source data record representation
 *
 * @author ynli
 */
class KafkaDataWriter<S> implements DataWriter<S, GenericRecord> {

    @Override
    public void write(S sourceRecord) throws IOException {
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
