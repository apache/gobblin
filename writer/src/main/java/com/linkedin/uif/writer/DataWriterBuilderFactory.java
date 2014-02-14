package com.linkedin.uif.writer;

/**
 * A factory class for {@link DataWriterBuilder}.
 *
 * @author ynli
 */
public class DataWriterBuilderFactory {

    /**
     * Create a new {@link DataWriterBuilder}.
     *
     * @param format Writer output format
     * @return newly created {@link DataWriterBuilder}
     */
    public DataWriterBuilder newDataWriterBuilder(WriterOutputFormat format) {
        switch (format) {
            case AVRO:
                return new AvroDataWriterBuilder();
            default:
                throw new RuntimeException();
        }
    }
}
