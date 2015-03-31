package gobblin.writer;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Lists;

import gobblin.configuration.State;

public class AvroHdfsDailyPartitionedWriter implements DataWriter<GenericRecord> {

  private List<AvroHdfsDataWriter> avroHdfsDataWriters;

  public AvroHdfsDailyPartitionedWriter(State properties, String relFilePath, String fileName, Schema schema, int branch)
      throws IOException {

    this.avroHdfsDataWriters = Lists.newArrayList();// new AvroHdfsDataWriter(properties, relFilePath, fileName, schema, branch);
  }

  @Override
  public void close() throws IOException {
    for (AvroHdfsDataWriter avroHdfsDataWriter : avroHdfsDataWriters) {
      avroHdfsDataWriter.close();
    }
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commit() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cleanup() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public long recordsWritten() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}
