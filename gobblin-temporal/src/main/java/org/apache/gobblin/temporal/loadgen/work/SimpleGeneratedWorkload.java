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

package org.apache.gobblin.temporal.loadgen.work;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import org.apache.gobblin.temporal.util.nesting.work.SeqBackedWorkSpan;
import org.apache.gobblin.temporal.util.nesting.work.Workload;


/** Example, illustration workload that synthesizes its work items; genuine {@link Workload}s generally arise from query/calc */
@lombok.AllArgsConstructor(access = AccessLevel.PRIVATE)
@lombok.NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@lombok.ToString
public class SimpleGeneratedWorkload implements Workload<IllustrationItem> {
  private int numItems;

  /** Factory method */
  public static SimpleGeneratedWorkload createAs(final int numItems) {
    return new SimpleGeneratedWorkload(numItems);
  }

  @Override
  public Optional<Workload.WorkSpan<IllustrationItem>> getSpan(final int startIndex, final int numElements) {
    if (startIndex >= numItems || startIndex < 0) {
      return Optional.empty();
    } else {
      List<IllustrationItem> elems = IntStream.range(startIndex, Math.min(startIndex + numElements, numItems))
          .mapToObj(n -> new IllustrationItem("item-" + n + "-of-" + numItems))
          .collect(Collectors.toList());
      return Optional.of(new SeqBackedWorkSpan<>(elems, startIndex));
    }
  }

  @Override
  public boolean isIndexKnownToExceed(final int index) {
    return isDefiniteSize() && index >= numItems;
  }

  @Override
  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public boolean isDefiniteSize() {
    return true;
  }
}
