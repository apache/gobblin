package gobblin.compaction.verify;

import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import lombok.AllArgsConstructor;


/**
 * An interface which represents a generic verifier for compaction
 */
public interface CompactionVerifier<D extends Dataset> {
   String COMPACTION_VERIFIER_PREFIX = "compaction-verifier-";

   boolean verify(D dataset);

   String getName();
}