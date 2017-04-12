package gobblin.compaction.verify;

import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.AllArgsConstructor;

/**
 * A simple class which verify current dataset belongs to a specific time range. Will skip to do
 * compaction if dataset is not in a correct time range.
 */
@AllArgsConstructor
public class CompactionTimeRangeVerifier implements CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_TIME_RANGE = COMPACTION_VERIFIER_PREFIX + "time-range";

  protected State state;

  public boolean verify (FileSystemDataset dataset) {
    //TODO: check if current dataset is between a specific range
    return true;
  }

  public String getName() {
    return COMPACTION_VERIFIER_TIME_RANGE;
  }
}
