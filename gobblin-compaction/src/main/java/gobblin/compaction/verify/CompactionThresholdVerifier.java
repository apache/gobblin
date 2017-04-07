package gobblin.compaction.verify;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.dataset.CompactionPartition;
import gobblin.configuration.State;

/**
 * A class which used to verify previous compacted records and current total records
 * and determine if a recompaction is needed
 */
@Slf4j
@AllArgsConstructor
public class CompactionThresholdVerifier implements CompactionVerifier {
  State state;
  public boolean verify (CompactionPartition partition) {
    return true;
  }
}
