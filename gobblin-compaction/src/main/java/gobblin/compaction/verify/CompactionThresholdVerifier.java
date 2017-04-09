package gobblin.compaction.verify;

import gobblin.dataset.FileSystemDataset;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;

/**
 * A class which counts previous compacted records and compare to the new total records.
 * If difference exceeds the user specified tolerance, a compaction needs to be triggered.
 */
@Slf4j
public class CompactionThresholdVerifier extends CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_THRESHOLD = COMPACTION_VERIFIER_PREFIX + "threshold";

  public CompactionThresholdVerifier (State state) {
    super (state);
  }

  public boolean verify (FileSystemDataset dataset) {
    //TODO: check previous compacted records and compare to current new records
    return true;
  }

  public String getName() {
    return COMPACTION_VERIFIER_THRESHOLD;
  }
}
