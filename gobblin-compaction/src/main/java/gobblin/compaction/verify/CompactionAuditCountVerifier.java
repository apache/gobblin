package gobblin.compaction.verify;

import gobblin.dataset.FileSystemDataset;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;

/**
 * A verifier compare audit count from upstream tier to the local record count
 */
@Slf4j
public class CompactionAuditCountVerifier extends CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_AUDIT_COUNT = COMPACTION_VERIFIER_PREFIX + "audit-count";

  public CompactionAuditCountVerifier(State state) {
    super(state);
  }

  /**
   * Compare audit count from upstream tier to the local record count
   * @return true if the difference is below a user defined tolerance
   */
  public boolean verify (FileSystemDataset dataset) {
    //TODO: compare upstream audit count and current new records
    return true;
  }

  /**
   * Get this verifier name
   * @return name of this verifier
   */
  public String getName() {
    return COMPACTION_VERIFIER_AUDIT_COUNT;
  }
}
