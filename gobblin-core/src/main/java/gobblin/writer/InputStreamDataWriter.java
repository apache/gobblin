package gobblin.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;

import java.io.IOException;
import java.net.URI;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;


@Slf4j
public class InputStreamDataWriter implements DataWriter<FileAwareInputStream> {

  protected long bytesWritten = 0;
  protected long filesWritten = 0;
  protected final State state;
  protected final FileSystem fs;
  protected final Path stagingDir;
  protected final Path outputDir;
  protected Closer closer = Closer.create() ;

  public InputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    this.state = state;

    Configuration conf = new Configuration();
    String uri =
        this.state
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
                branchId), ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), conf);
    this.stagingDir = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
    this.outputDir = WriterUtils.getWriterOutputDir(state, numBranches, branchId);
  }

  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    closer.register(fileAwareInputStream.getInputStream());
    this.fs.mkdirs(fileAwareInputStream.getFile().getDestination().getParent());
    IOUtils.copyLarge(fileAwareInputStream.getInputStream(),
        fs.create(fileAwareInputStream.getFile().getDestination(), true));
    filesWritten++;

    this.commit(fileAwareInputStream.getFile());
  }

  protected void commit(CopyableFile file) throws IOException {
    if (!this.fs.exists(file.getDestination())) {
      throw new IOException(String.format("File %s does not exist", file.getDestination()));
    }

    Path stagingFile = new Path(this.stagingDir, file.getDestination().getName());
    Path outputFile = new Path(this.outputDir, file.getDestination().getName());

    log.info(String.format("Moving data from %s to %s", stagingFile, outputFile));

    if (this.fs.exists(outputFile)) {
      log.warn(String.format("Task output file %s already exists", outputFile));
      HadoopUtils.deletePath(this.fs, outputFile, true);
    }

    HadoopUtils.renamePath(this.fs, stagingFile, outputFile);

    log.info(String.format("Moved data from %s to %s", stagingFile, outputFile));
  }

  @Override
  public long recordsWritten() {
    return filesWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    return bytesWritten;
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

  @Override
  public void commit() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {
  }
}
