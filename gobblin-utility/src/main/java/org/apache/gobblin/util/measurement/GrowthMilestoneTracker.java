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

package org.apache.gobblin.util.measurement;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.LongStream;


/** Stateful class to track growth/accumulation/"high watermark" against milestones */
public class GrowthMilestoneTracker {
  private final Iterator<Long> milestoneSequence = createMilestoneSequence();
  private Long nextMilestone = milestoneSequence.next();

  /** @return whether `n >=` the next monotonically increasing milestone (with no effort to handle wrap-around) */
  public final boolean isAnotherMilestone(long n) {
    return this.calcLargestNewMilestone(n).isPresent();
  }

  /** @return largest monotonically increasing milestone iff `n >=` some new one (no effort to handle wrap-around) */
  public final Optional<Long> calcLargestNewMilestone(long n) {
    if (n < this.nextMilestone) {
      return Optional.empty();
    }
    Long largestMilestoneAchieved;
    do {
      largestMilestoneAchieved = this.nextMilestone;
      this.nextMilestone = this.milestoneSequence.hasNext() ? this.milestoneSequence.next() : Long.MAX_VALUE;
    } while (n >= this.nextMilestone);
    return Optional.of(largestMilestoneAchieved);
  }

  /**
   * @return positive monotonically increasing milestones, for {@link GrowthMilestoneTracker#isAnotherMilestone(long)}
   * to track against; if/whenever exhausted, {@link Long#MAX_VALUE} becomes stand-in thereafter
   * DEFAULT SEQ: [1, 10, 100, 1000, 10k, 15k, 20k, 25k, 30k, ..., 50k, 75k, 100k, 125k, ..., 250k, 300k, 350k, ... )
   */
  protected Iterator<Long> createMilestoneSequence() {
    LongStream initially = LongStream.iterate(1L, i -> i * 10).limit((long) Math.log10(1000));
    LongStream next = LongStream.rangeClosed(2L, 9L).map(i -> i * 5000); // 10k - 45k
    LongStream then = LongStream.rangeClosed(2L, 9L).map(i -> i * 25000); // 50k - 225k
    LongStream thereafter = LongStream.iterate(250000L, i -> i + 50000);
    return
        LongStream.concat(initially,
            LongStream.concat(next,
                LongStream.concat(then, thereafter))
        ).iterator();
  }
}
