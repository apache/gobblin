package gobblin.compaction.verify;

import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import lombok.AllArgsConstructor;


/**
 * A class which represents a generic verifier for compaction
 */
@AllArgsConstructor
public abstract class CompactionVerifier<D extends Dataset> {
   public final static String COMPACTION_VERIFIER_PREFIX = "compaction-verifier-";

   protected final State state;

   public abstract boolean verify(D dataset);

   public abstract String getName();
}