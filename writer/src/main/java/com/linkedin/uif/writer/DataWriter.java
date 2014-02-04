package com.linkedin.uif.writer;

import java.io.IOException;

/**
 * An interface for UIF data writers.
 *
 * @param <T> type of source data record representation
 */
public interface DataWriter<T> {

    /**
     * Write a source data record in Avro format using the given converter.
     *
     * @param sourceRecord source data record
     * @throws IOException if there is anything wrong writing the record
     */
    public void write(T sourceRecord) throws IOException;

    /**
     * Close this writer.
     *
     * @throws IOException if there is anything wrong closing the writer
     */
    public void close() throws IOException;

    /**
     * Commit the data written.
     *
     * @throws IOException if there is anything wrong committing the output
     */
    public void commit() throws IOException;

    /**
     * Get the number of records written.
     *
     * @return number of records written
     */
    public long recordsWritten();
}
