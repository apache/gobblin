package gobblin.writer;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Extends the {@link AvroDataWriterBuilder} class, and is used to writer Avro data in a date-partitioned fashion. There
 * is currently only support for writing data to HDFS.
 */
public class AvroTimePartitionedWriterBuilder extends AvroDataWriterBuilder {

  @Override
  public DataWriter<GenericRecord> build() throws IOException {

    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

    switch (this.destination.getType()) {
      case HDFS:
        return new AvroHdfsTimePartitionedWriter(this.destination, this.writerId, this.schema, this.format, this.branch);
      case KAFKA:
        throw new UnsupportedOperationException("The builder " + this.getClass().getName() + " cannot write to "
            + this.destination.getType());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }
}
