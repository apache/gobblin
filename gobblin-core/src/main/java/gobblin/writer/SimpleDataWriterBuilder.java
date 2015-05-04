package gobblin.writer;

import gobblin.configuration.State;
import gobblin.util.WriterUtils;

import java.io.IOException;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes bytes.
 *
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriterBuilder extends DataWriterBuilder<String, byte[]> {
  /**
   * Build a {@link gobblin.writer.DataWriter}.
   *
   * @return the built {@link gobblin.writer.DataWriter}
   * @throws java.io.IOException if there is anything wrong building the writer
   */
  @Override
  public DataWriter<byte[]> build() throws IOException {
    State properties = this.destination.getProperties();
    String fileName =
            WriterUtils.getWriterFileName(properties, this.branches, this.branch, this.writerId, this.format.getExtension());
    return new SimpleDataWriter(properties, fileName, this.branches, this.branch);
  }
}
