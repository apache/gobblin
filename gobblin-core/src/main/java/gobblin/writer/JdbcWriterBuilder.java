package gobblin.writer;

import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntrySchema;

import java.io.IOException;

import org.apache.avro.Schema;

public class JdbcWriterBuilder extends PartitionAwareDataWriterBuilder<JdbcEntrySchema, JdbcEntryData> {

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return false;
  }

  @Override
  public DataWriter<JdbcEntryData> build() throws IOException {
    return new JdbcWriter(this);
  }

}