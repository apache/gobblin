package gobblin.compaction.dataset;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.verify.CompactionAuditCountVerifier;
import gobblin.compaction.verify.CompactionThresholdVerifier;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.configuration.State;

/**
 * This class represents a sort of utilities {@link CompactionPartition} will use to parse or verify the data.
 *
 * A {@link CompactionParser} is provided
 * A map of {@link CompactionVerifier} is provided
 */
@Slf4j
@AllArgsConstructor
public class CompactionSuite {
  State state;

  public CompactionParser getParser () {
    return new CompactionParser(state);
  }

  public Map<String, CompactionVerifier> getVerifiers() {
    HashMap<String, CompactionVerifier> map = new HashMap();
    map.put(CompactionVerifier.COMPACTION_VERIFIER_THRESHOLD, new CompactionThresholdVerifier(state));
    map.put(CompactionVerifier.COMPACTION_VERIFIER_AUDIT_COUNT, new CompactionAuditCountVerifier(state));
    map.put(CompactionVerifier.COMPACTION_VERIFIER_DUMMY, new CompactionVerifier() {
      @Override
      public boolean verify(CompactionPartition partition) {
        return true;
      }
    });
    return map;
  }

}
