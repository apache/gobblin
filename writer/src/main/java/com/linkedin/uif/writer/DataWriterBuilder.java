package com.linkedin.uif.writer;

import java.io.IOException;

import com.linkedin.uif.converter.DataConverter;
import com.linkedin.uif.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

/**
 * A builder class for {@link DataWriter}.
 *
 * @param <SI> type of source schema representation
 * @param <SO> target schema type
 * @param <DI> type of source data record representation
 * @param <DO> target record data type
 *
 * @author ynli
 */
public abstract class DataWriterBuilder<SI, SO, DI, DO> {

    protected Destination destination;
    protected String writerId;
    protected WriterOutputFormat format;
    protected DataConverter<DI, DO> dataConverter;
    protected SchemaConverter<SI, SO> schemaConverter;
    protected SI sourceSchema;
    protected SchemaType schemaType;

    /**
     * Tell the writer the destination to write to.
     *
     * @param destination destination to write to
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<SI, SO, DI, DO> writeTo(Destination destination) {
        this.destination = destination;
        return this;
    }

    /**
     * Tell the writer the output format of type {@link WriterOutputFormat}
     *
     * @param format output format of the writer
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<SI, SO, DI, DO> writeInFormat(WriterOutputFormat format) {
        this.format = format;
        return this;
    }

    /**
     * Give the writer a unique ID.
     *
     * @param writerId unique writer ID
     * @return this {@link DataWriterBuilder} instance
     */
    public DataWriterBuilder<SI, SO, DI, DO> writerId(String writerId) {
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
    public DataWriterBuilder<SI, SO, DI, DO> useDataConverter(
            DataConverter<DI, DO> dataConverter) {

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
    public DataWriterBuilder<SI, SO, DI, DO> useSchemaConverter(
            SchemaConverter<SI, SO> schemaConverter) {

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
    public DataWriterBuilder<SI, SO, DI, DO> withSourceSchema(
            SI sourceSchema, SchemaType schemaType) {

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
    @SuppressWarnings("unchecked")
    public abstract DataWriter<DI, DO> build() throws IOException;
}
