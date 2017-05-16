package gobblin.compaction.action;

import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

@Slf4j
@AllArgsConstructor
public class CompactionMarkDirectoryAction implements CompactionCompleteAction<FileSystemDataset> {
  protected State state;
  private CompactionAvroJobConfigurator configurator;
  private FileSystem fs;

  public CompactionMarkDirectoryAction(State state, CompactionAvroJobConfigurator configurator) {
    this.state = state;
    this.configurator = configurator;
    this.fs = configurator.getFs();
  }

  public void onCompactionJobComplete (FileSystemDataset dataset) throws IOException {
    boolean renamingRequired = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
            MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

    if (renamingRequired) {
      Collection<Path> paths = configurator.getMapReduceInputPaths();
      for (Path path: paths) {
        Path newPath = new Path (path.getParent(), path.getName() + MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX);
        log.info("[{}] Renaming {} to {}", dataset.datasetURN(), path, newPath);
        fs.rename(path, newPath);
      }
    }
  }
}
