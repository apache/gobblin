package com.linkedin.uif.writer;

import java.io.IOException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * A factory class for {@link DataWriterBuilder}.
 *
 * @author ynli
 */
public class DataWriterBuilderFactory {

    /**
     * Create a new {@link DataWriterBuilder}.
     *
     * @param state {@link State} object carrying the configuration properties
     * @return newly created {@link DataWriterBuilder}
     */
    public DataWriterBuilder newDataWriterBuilder(State state) throws IOException {
        try {
            return (DataWriterBuilder) Class.forName(
                    state.getProp(
                            ConfigurationKeys.WRITER_BUILDER_CLASS,
                            ConfigurationKeys.DEFAULT_WRITER_BUILDER_CLASS))
                    .newInstance();
        } catch (Exception e) {
            throw new IOException("Failed to instantiate a DataWriterBuilder", e);
        }
    }
}
