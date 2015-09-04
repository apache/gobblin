package gobblin.source.extractor.hadoop;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.utils.BufferedRawByteIterator;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Implementation of {@link gobblin.source.extractor.Extractor} that reads a file as a sequence of byte arrays.
 * Uses a {@link gobblin.source.extractor.utils.BufferedRawByteIterator}.
 */
public class RawByteExtractor implements Extractor<String, byte[]> {

  private static final int BYTE_BUFFER_SIZE = 10000;

  private final InputStream is;
  private final BufferedRawByteIterator it;
  private final FileStatus fileStatus;

  public RawByteExtractor(FileSystem fs, Path path) throws IOException {
    this.fileStatus = fs.getFileStatus(path);
    this.is = fs.open(path);
    this.it = new BufferedRawByteIterator(this.is, BYTE_BUFFER_SIZE);
  }

  /**
   * @return Constant string schema.
   * @throws IOException
   */
  @Override public String getSchema() throws IOException {
    return "RawBytes";
  }

  @Override public byte[] readRecord(@Deprecated byte[] reuse) throws DataRecordException, IOException {
    if(this.it.hasNext()) {
      return this.it.next();
    } else {
      return null;
    }
  }

  @Override public long getExpectedRecordCount() {
    return fileStatus.getLen() / BYTE_BUFFER_SIZE + 1;
  }

  @Override public long getHighWatermark() {
    return 0;
  }

  @Override public void close() throws IOException {
    this.is.close();
  }
}
