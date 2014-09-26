package com.linkedin.uif.writer;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

/**
 * An implementation of {@link DataWriter} that writes to a Kafka topic.
 *
 * @author ynli
 */
class AvroKafkaDataWriter implements DataWriter<GenericRecord> {

    @Override
    public void write(GenericRecord record) throws IOException {
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

    @Override
    public long bytesWritten() {
        return 0;
    }
}
