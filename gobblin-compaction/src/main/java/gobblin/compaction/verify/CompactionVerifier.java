package gobblin.compaction.verify;

import gobblin.dataset.Dataset;

/**
 * An interface which represents a generic verifier for compaction
 */
public interface CompactionVerifier<D extends Dataset> {
   String COMPACTION_VERIFIER_PREFIX = "compaction-verifier-";
   String COMPACTION_VERIFICATION_TIMEOUT_MINUTES = "compaction.verification.timeoutMinutes";
   String COMPACTION_VERIFICATION_ITERATION_COUNT_LIMIT = "compaction.verification.iteration.countLimit";
   String COMPACTION_VERIFICATION_THREADS= "compaction.verification.threads";
   boolean verify(D dataset);
   boolean isRetriable ();
   String getName();
}