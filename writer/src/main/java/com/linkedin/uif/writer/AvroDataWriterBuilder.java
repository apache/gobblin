package com.linkedin.uif.writer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.converter.SchemaConversionException;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes
 * in Avro format.
 *
 * @param <SI> type of source schema representation
 * @param <DI> type of source data record representation
 *
 * @author ynli
 */
public class AvroDataWriterBuilder<SI, DI> extends
        DataWriterBuilder<SI, Schema, DI, GenericRecord> {
    
    public DataWriter<DI, GenericRecord> build() throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.sourceSchema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

        // Convert the source schema to Avro schema
        Schema schema;
        try {
            if (Optional.fromNullable(this.schemaConverter).isPresent()) {
                schema = this.schemaConverter.convert(this.sourceSchema);
            } else {
                schema = new Schema.Parser().parse(this.sourceSchema.toString());
            }
        } catch (SchemaConversionException e) {
            throw new IOException("Failed to convert the source schema: " + this.sourceSchema, e);
        }

        switch (this.destination.getType()) {
            case HDFS:
                State properties = this.destination.getProperties();
                // Add the writer ID to the file name so each writer writes to a different
                // file of the same file group defined by the given file name
                String fileName = String.format(
                       "%s.%s.%s",
                        properties.getProp(ConfigurationKeys.WRITER_FILE_NAME, "part"),
                       this.writerId,
                       this.format.getExtension());
                return new AvroHdfsDataWriter<DI>(properties, this.filePath, fileName,
                        this.dataConverter, schema);
            case KAFKA:
                return new KafkaDataWriter<DI>();
            default:
                throw new RuntimeException("Unknown destination type: " + this.destination.getType());
        }
    }
}
