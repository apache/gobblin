package gobblin.compaction.verify;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.dataset.CompactionParser;
import gobblin.compaction.dataset.CompactionPartition;
import gobblin.configuration.State;

/**
 * A verifier compare audit count from upstream tier and local record count
 */
@Slf4j
@AllArgsConstructor
public class CompactionAuditCountVerifier implements CompactionVerifier {
  State state;
  public boolean verify (CompactionPartition partition) {
    //TODO: compare upstream audit count
    return true;
  }
}
