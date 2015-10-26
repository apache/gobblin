package gobblin.data.management.copy.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import java.io.IOException;


public class InputStreamDataWriterBuilder extends DataWriterBuilder<String, FileAwareInputStream> {
  @Override
  public DataWriter<FileAwareInputStream> build() throws IOException {
    State properties = this.destination.getProperties();
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.writerId);
    return new ArchivedInputStreamDataWriter(properties, this.branches, this.branch);
  }
}
