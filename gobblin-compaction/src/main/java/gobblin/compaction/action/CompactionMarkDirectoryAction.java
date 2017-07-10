package gobblin.compaction.action;

import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.FileSystemDataset;
import gobblin.metrics.event.EventSubmitter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Joiner;


@Slf4j
@AllArgsConstructor
public class CompactionMarkDirectoryAction implements CompactionCompleteAction<FileSystemDataset> {
  protected State state;
  private CompactionAvroJobConfigurator configurator;
  private FileSystem fs;
  private EventSubmitter eventSubmitter;
  public CompactionMarkDirectoryAction(State state, CompactionAvroJobConfigurator configurator) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
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

      // submit events if directory is renamed
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = new HashMap<>();
        eventMetadataMap.put("datasetUrn", dataset.datasetURN());
        eventMetadataMap.put(CompactionSlaEventHelper.RENAME_DIR_PATHS, Joiner.on(',').join(paths));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_MARK_DIR_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}
