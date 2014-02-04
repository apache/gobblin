package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.uif.writer.conf.WriterConfig;
import com.linkedin.uif.writer.converter.DataConverter;
import com.linkedin.uif.writer.converter.SchemaConversionException;
import com.linkedin.uif.writer.hdfs.HdfsDataWriter;
import com.linkedin.uif.writer.kafka.KafkaDataWriter;
import com.linkedin.uif.writer.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

import org.apache.avro.Schema;

/**
 * A builder class for {@link DataWriter}.
 *
 * @param <D> type of source data record representation
 * @param <S> type of source schema representation
 */
public class DataWriterBuilder<D, S> {

    private Destination destination;
    private String writerId;
    private String dataName;
    private String dataStartTime;
    private String dataEndTime;
    private long count;
    private DataConverter<D> dataConverter;
    private SchemaConverter<S> schemaConverter;
    private S sourceSchema;
    private SchemaType schemaType;

    /**
     * Create a new {@link DataWriterBuilder}.
     *
     * @return a new {@link DataWriterBuilder}
     */
    public static DataWriterBuilder newBuilder() {
        return new DataWriterBuilder();
    }

    /**
     * Tell the writer the destination to write to.
     *
     * @param destination destination to write to
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder writeTo(Destination destination) {
        this.destination = destination;
        return this;
    }

    /**
     * Give the writer a unique ID.
     *
     * @param writerId unique writer ID
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder writerId(String writerId) {
        this.writerId = writerId;
        return this;
    }

    /**
     * Tell the writer the name of the data.
     *
     * @param name name of the data
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder dataName(String name) {
        this.dataName = name;
        return this;
    }

    /**
     * Tell the writer the start time of the data.
     *
     * @param startTime start time of the data
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder dataStartTime(String startTime) {
        this.dataStartTime = startTime;
        return this;
    }

    /**
     * Tell the writer the end time of the data.
     *
     * @param endTime end time of the data
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder dataEndTime(String endTime) {
        this.dataEndTime = endTime;
        return this;
    }

    /**
     * Tell the writer what {@link DataConverter} to use.
     *
     * @param dataConverter converter to convert the source record to a
     *                      {@link org.apache.avro.generic.GenericRecord}
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder useDataConverter(DataConverter<D> dataConverter) {
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
    public DataWriterBuilder useSchemaConverter(SchemaConverter<S> schemaConverter) {
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
    public DataWriterBuilder dataSchema(S sourceSchema, SchemaType schemaType) {
        this.sourceSchema = sourceSchema;
        this.schemaType = schemaType;
        return this;
    }

    /**
     * Tell the writer the expected total count of records.
     *
     * @param count expected total count of records
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder totalCount(long count) {
        this.count = count;
        return this;
    }

    /**
     * Build a {@link DataWriter}.
     *
     * @throws IOException if there is anything wrong building the writer
     * @return the built {@link DataWriter}
     */
    public DataWriter build() throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.dataConverter);
        Preconditions.checkNotNull(this.schemaConverter);
        Preconditions.checkNotNull(this.sourceSchema);

        switch (this.destination.getType()) {
            case HDFS:
                Properties properties = this.destination.getProperties();
                String uri = properties.getProperty(WriterConfig.FILE_SYSTEM_URI_KEY);
                String stagingDir = properties.getProperty(WriterConfig.STAGING_DIR_KEY,
                        WriterConfig.DEFAULT_STAGING_DIR);
                String outputDir = properties.getProperty(WriterConfig.OUTPUT_DIR_KEY,
                        WriterConfig.DEFAULT_OUTPUT_DIR);
                String fileName = properties.getProperty(WriterConfig.FILE_NAME_KEY) +
                        "." + this.writerId;
                int bufferSize = Integer.parseInt(properties.getProperty(
                        WriterConfig.BUFFER_SIZE_KEY, WriterConfig.DEFAULT_BUFFER_SIZE));

                Schema schema;
                try {
                    schema = this.schemaConverter.convert(this.sourceSchema);
                } catch (SchemaConversionException e) {
                    throw new IOException("Failed to convert the source schema: " +
                            this.sourceSchema);
                }

                return new HdfsDataWriter<D>(URI.create(uri), stagingDir, outputDir,
                        fileName, bufferSize, this.dataConverter, schema);
            case KAFKA:
                return new KafkaDataWriter();
            default:
                throw new RuntimeException("Unknown destination type: " +
                        this.destination.getType());
        }
    }
}
