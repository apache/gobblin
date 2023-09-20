package org.apache.gobblin.temporal.workflows;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Iterator;
import java.util.Optional;


/**
 * An assemblage of "work", modeled as sequential "task" specifications.  Given Temporal's required determinism, tasks
 * and task spans should remain unchanged, with stable sequential ordering.  This need not constrain `Workload`s to
 * eager, advance elaboration: "streaming" definition is possible, so long as producing a deterministic result.
 *
 * A actual, real-world workload might correspond to datastore contents, such as records serialized into HDFS files
 * or ordered DB query results.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class") // to handle impls

public interface Workload<TASK> {

    /**
     * @return a sequential sub-sequence, from `startIndex` (0-based), unless it falls beyond the underlying sequence
     * NOTE: this is a blocking call that forces elaboration: `TaskSpan.getNumElems() < numElements` signifies end of seq
     */
    Optional<TaskSpan<TASK>> getSpan(int startIndex, int numElements);

    /** Non-blocking, best-effort advice: to support non-strict elaboration, does NOT guarantee `index` will not exceed */
    boolean isIndexKnownToExceed(int index);

    default boolean isDefiniteSize() {
        return false;
    }

    /** Logical sub-sequence 'slice' of contiguous "tasks" */
    public interface TaskSpan<T> extends Iterator<T> {
        int getNumElems();
    }
}
