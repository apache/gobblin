/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.temporal.workflows;

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
