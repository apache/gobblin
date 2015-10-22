package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ParallelRunner;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class CopyDataPublisher extends BaseDataPublisher {

  public CopyDataPublisher(State state) throws IOException {
    super(state);
  }

  @Override
  protected void rename(Path writerOutputDir, Path publisherOutputDir, int branchId) throws IOException {

    FileSystem fs = this.fileSystemByBranches.get(branchId);
    for (FileStatus fileStatus : fs.listStatus(writerOutputDir)) {
      log.info(String.format("Publishing %s to %s", fileStatus.getPath(), new Path(publisherOutputDir,
          getNewFileName(fileStatus))));
      fs.rename(fileStatus.getPath(), new Path(publisherOutputDir, getNewFileName(fileStatus)));
    }
  }

  protected String getNewFileName(FileStatus fileStatus) {
    return fileStatus.getPath().getName();
  }

  @Override
  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    this.rename(writerOutputDir, publisherOutputDir, branchId);
  }

  @Override
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    return new Path(workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
  }
}
