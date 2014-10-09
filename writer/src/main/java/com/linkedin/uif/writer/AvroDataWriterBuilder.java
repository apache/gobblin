package com.linkedin.uif.writer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes in Avro format.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class AvroDataWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

    @Override
    public DataWriter<GenericRecord> build() throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.schema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

        switch (this.destination.getType()) {
            case HDFS:
                State properties = this.destination.getProperties();

                String filePath = properties.getProp(ConfigurationKeys.WRITER_FILE_PATH);
                // Add the writer ID to the file name so each writer writes to a different
                // file of the same file group defined by the given file name
                String fileName = String.format(
                       "%s.%s.%s",
                        properties.getProp(ConfigurationKeys.WRITER_FILE_NAME, "part"),
                       this.writerId,
                       this.format.getExtension());

                return new AvroHdfsDataWriter(properties, filePath, fileName, this.schema);
            case KAFKA:
                return new AvroKafkaDataWriter();
            default:
                throw new RuntimeException("Unknown destination type: " + this.destination.getType());
        }
    }
}
