package org.apache.gobblin.cluster.temporal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AccessLevel;

/** Example, illustration workload that synthesizes tasks; genuine {@link Workload}s likely arise from query/calc */
@lombok.AllArgsConstructor(access = AccessLevel.PRIVATE)
@lombok.NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@lombok.ToString
public class SimpleGeneratedWorkload implements Workload<IllustrationTask> {
    private int numTasks;

    /** Factory method */
    public static SimpleGeneratedWorkload createAs(final int numTasks) {
        return new SimpleGeneratedWorkload(numTasks);
    }

    @Override
    public Optional<Workload.TaskSpan<IllustrationTask>> getSpan(final int startIndex, final int numElements) {
        if (startIndex >= numTasks || startIndex < 0) {
            return Optional.empty();
        } else {
            List<IllustrationTask> elems = IntStream.range(startIndex, Math.min(startIndex + numElements, numTasks))
                    .mapToObj(n -> new IllustrationTask("task-" + n + "-of-" + numTasks))
                    .collect(Collectors.toList());
            return Optional.of(new CollectionBackedTaskSpan<>(elems, startIndex));
        }
    }

    @Override
    public boolean isIndexKnownToExceed(final int index) {
        return isDefiniteSize() && index >= numTasks;
    }

    @Override
    @JsonIgnore // (because no-arg method resembles 'java bean property')
    public boolean isDefiniteSize() {
        return true;
    }
}
