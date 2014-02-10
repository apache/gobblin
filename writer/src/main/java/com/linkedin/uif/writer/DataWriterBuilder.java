package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.converter.DataConverter;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

import org.apache.avro.Schema;

/**
 * A builder class for {@link DataWriter}.
 *
 * @param <S> type of source schema representation
 * @param <D> type of source data record representation
 */
public class DataWriterBuilder<S, D> {

    private Destination destination;
    private String writerId;
    private DataConverter<D> dataConverter;
    private SchemaConverter<S> schemaConverter;
    private S sourceSchema;
    private SchemaType schemaType;

    /**
     * Create a new {@link DataWriterBuilder}.
     *
     * @return a new {@link DataWriterBuilder}
     */
    public static <S, D> DataWriterBuilder<S, D> newBuilder() {
        return new DataWriterBuilder<S, D>();
    }

    /**
     * Tell the writer the destination to write to.
     *
     * @param destination destination to write to
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<S, D> writeTo(Destination destination) {
        this.destination = destination;
        return this;
    }

    /**
     * Give the writer a unique ID.
     *
     * @param writerId unique writer ID
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<S, D> writerId(String writerId) {
        this.writerId = writerId;
        return this;
    }

    /**
     * Tell the writer what {@link DataConverter} to use.
     *
     * @param dataConverter converter to convert the source record to a
     *                      {@link org.apache.avro.generic.GenericRecord}
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<S, D> useDataConverter(DataConverter<D> dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    /**
     * Tell the writer what {@link SchemaConverter} to use.
     *
     * @param schemaConverter converter to convert the source schema to a
     *                        {@link org.apache.avro.Schema}
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<S, D> useSchemaConverter(SchemaConverter<S> schemaConverter) {
        this.schemaConverter = schemaConverter;
        return this;
    }

    /**
     * Tell the writer the data schema.
     *
     * @param sourceSchema source data schema
     * @param schemaType type of schema expected and used by the target
     *                   consumer, e.g., Lumos
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<S, D> dataSchema(S sourceSchema, SchemaType schemaType) {
        this.sourceSchema = sourceSchema;
        this.schemaType = schemaType;
        return this;
    }

    /**
     * Build a {@link DataWriter}.
     *
     * @throws IOException if there is anything wrong building the writer
     * @return the built {@link DataWriter}
     */
    public DataWriter<D> build() throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.dataConverter);
        Preconditions.checkNotNull(this.schemaConverter);
        Preconditions.checkNotNull(this.sourceSchema);
        Preconditions.checkNotNull(this.schemaType);

        // Convert the source schema to Avro schema
        Schema schema;
        try {
            schema = this.schemaConverter.convert(this.sourceSchema);
        } catch (SchemaConversionException e) {
            throw new IOException("Failed to convert the source schema: " +
                    this.sourceSchema);
        }

        // Perform schema validation
        if (!this.schemaType.getSchemaValidator().validate(schema)) {
            throw new IOException(String.format(
                    "Schema of type %s could not be validated", this.schemaType.name()));
        }

        switch (this.destination.getType()) {
            case HDFS:
                Properties properties = this.destination.getProperties();
                String uri = properties.getProperty(ConfigurationKeys.FILE_SYSTEM_URI_KEY);
                String stagingDir = properties.getProperty(ConfigurationKeys.STAGING_DIR_KEY,
                        ConfigurationKeys.DEFAULT_STAGING_DIR);
                String outputDir = properties.getProperty(ConfigurationKeys.OUTPUT_DIR_KEY,
                        ConfigurationKeys.DEFAULT_OUTPUT_DIR);
                // Add the writer ID to the file name so each writer writes to a different
                // file of the same file group defined by the given file name
                String fileName = properties.getProperty(ConfigurationKeys.FILE_NAME_KEY) +
                        "." + this.writerId;
                int bufferSize = Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.BUFFER_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_BUFFER_SIZE));

                return new HdfsDataWriter<D>(URI.create(uri), stagingDir, outputDir,
                        fileName, bufferSize, this.dataConverter, schema);
            case KAFKA:
                return new KafkaDataWriter<D>();
            default:
                throw new RuntimeException("Unknown destination type: " +
                        this.destination.getType());
        }
    }
}
