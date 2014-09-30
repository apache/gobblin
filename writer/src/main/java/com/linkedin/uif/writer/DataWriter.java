package com.linkedin.uif.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface for data writers.
 *
 * @param <D> data record type
 *
 * @author ynli
 */
public interface DataWriter<D> extends Closeable {

    /**
     * Write a source data record in Avro format using the given converter.
     *
     * @param record data record to write
     * @throws IOException if there is anything wrong writing the record
     */
    public void write(D record) throws IOException;

    /**
     * Commit the data written.
     *
     * @throws IOException if there is anything wrong committing the output
     */
    public void commit() throws IOException;

    /**
     * Cleanup context/resources.
     *
     * @throws IOException if there is anything wrong doing cleanup.
     */
    public void cleanup() throws IOException;

    /**
     * Get the number of records written.
     *
     * @return number of records written
     */
    public long recordsWritten();

    /**
     * Get the number of bytes written.
     *
     * <p>
     *     This method should ONLY be called after {@link DataWriter#commit()}
     *     is called.
     * </p>
     *
     * @return number of bytes written
     */
    public long bytesWritten() throws IOException;
}
