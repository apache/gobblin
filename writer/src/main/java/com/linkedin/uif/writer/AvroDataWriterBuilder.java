package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.ConfigurationKeys;
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
        Preconditions.checkNotNull(this.dataConverter);
        Preconditions.checkNotNull(this.schemaConverter);
        Preconditions.checkNotNull(this.sourceSchema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

        // Convert the source schema to Avro schema
        Schema schema;
        try {
            schema = this.schemaConverter.convert(this.sourceSchema);
        } catch (SchemaConversionException e) {
            throw new IOException("Failed to convert the source schema: " +
                    this.sourceSchema);
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

                return new AvroHdfsDataWriter<DI>(URI.create(uri), stagingDir,
                        outputDir, fileName, bufferSize, this.dataConverter, schema);
            case KAFKA:
                return new KafkaDataWriter<DI>();
            default:
                throw new RuntimeException("Unknown destination type: " +
                        this.destination.getType());
        }
    }
}
