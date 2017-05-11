package gobblin.compaction.verify;

import gobblin.dataset.Dataset;

/**
 * An interface which represents a generic verifier for compaction
 */
public interface CompactionVerifier<D extends Dataset> {
   String COMPACTION_VERIFIER_PREFIX = "compaction-verifier-";

   boolean verify(D dataset);

   String getName();
}