package gobblin.compaction.verify;

import gobblin.compaction.dataset.CompactionPartition;

/**
 * And interface which represents a generic verifier for compaction
 */
public interface CompactionVerifier {
   String COMPACTION_VERIFIER_AUDIT_COUNT = "compaction-verifier-audit-count";
   String COMPACTION_VERIFIER_THRESHOLD = "compaction-verifier-threshold";
   String COMPACTION_VERIFIER_DUMMY = "compaction-verifier-dummy";
   boolean verify(CompactionPartition partition) ;
}