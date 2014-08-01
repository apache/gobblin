package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

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
        Preconditions.checkNotNull(this.sourceSchema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

        // Convert the source schema to Avro schema
        Schema schema;
        try {
            if (this.schemaConverter != null) {
                schema = this.schemaConverter.convert(this.sourceSchema);
            } else {
                schema = new Schema.Parser().parse(this.sourceSchema.toString());
            }
        } catch (SchemaConversionException e) {
            throw new IOException("Failed to convert the source schema: " +
                    this.sourceSchema, e);
        }

        switch (this.destination.getType()) {
            case HDFS:
                Properties destProps = this.destination.getProperties();

                String uri = destProps.getProperty(ConfigurationKeys.WRITER_FILE_SYSTEM_URI);
                String stagingDir = destProps.getProperty(ConfigurationKeys.WRITER_STAGING_DIR,
                        ConfigurationKeys.DEFAULT_STAGING_DIR) + Path.SEPARATOR + this.filePath;
                String outputDir = destProps.getProperty(ConfigurationKeys.WRITER_OUTPUT_DIR,
                        ConfigurationKeys.DEFAULT_OUTPUT_DIR) + Path.SEPARATOR + this.filePath;
                // Add the writer ID to the file name so each writer writes to a different
                // file of the same file group defined by the given file name
                String fileName = String.format(
                       "%s.%s.%s",
                       destProps.getProperty(ConfigurationKeys.WRITER_FILE_NAME, "part"),
                       this.writerId,
                       this.format.getExtension());

                int bufferSize = Integer.parseInt(destProps.getProperty(
                        ConfigurationKeys.WRITER_BUFFER_SIZE,
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
