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

package org.apache.gobblin.temporal.ddm.work;

import java.util.List;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Total size, counts, and size distributions for a collection of {@link MultiWorkUnit}s, both with regard to top-level (possibly multi) {@link WorkUnit}s
 * and individual constituent (purely {@link WorkUnit}s), where:
 *   * a top-level work unit is one with no parent - a root
 *   * a constituent work unit is one with no children - a leaf
 * @see org.apache.gobblin.util.WorkUnitSizeInfo
 */
@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WorkUnitsSizeSummary {
  // NOTE: `@NonNull` to include field in `@RequiredArgsConstructor`, despite - "warning: @NonNull is meaningless on a primitive... @RequiredArgsConstructor"
  @NonNull private long totalSize;
  @NonNull private long topLevelWorkUnitsCount;
  @NonNull private long constituentWorkUnitsCount;
  @NonNull private int quantilesCount;
  @NonNull private double quantilesWidth;
  @NonNull private List<Double> topLevelQuantilesMinSizes;
  @NonNull private List<Double> constituentQuantilesMinSizes;

  /** Total size, counts, means, and medians: the most telling measurements packaged for ready consumption / observability */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  @RequiredArgsConstructor
  public static class Distillation {
    @NonNull private long totalSize;
    @NonNull private long topLevelWorkUnitsCount;
    @NonNull private long constituentWorkUnitsCount;
    @NonNull private double topLevelWorkUnitsMeanSize;
    @NonNull private double constituentWorkUnitsMeanSize;
    @NonNull private double topLevelWorkUnitsMedianSize;
    @NonNull private double constituentWorkUnitsMedianSize;
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public Distillation distill() {
    return new Distillation(this.totalSize, this.topLevelWorkUnitsCount, this.constituentWorkUnitsCount,
        this.getTopLevelWorkUnitsMeanSize(), this.getConstituentWorkUnitsMeanSize(),
        this.getTopLevelWorkUnitsMedianSize(), this.getConstituentWorkUnitsMedianSize()
    );
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public double getTopLevelWorkUnitsMeanSize() {
    return this.topLevelWorkUnitsCount == 0 ? 0.0 : (this.totalSize * 1.0 / this.topLevelWorkUnitsCount);
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public double getConstituentWorkUnitsMeanSize() {
    return this.constituentWorkUnitsCount == 0 ? 0.0 : (this.totalSize * 1.0 / this.constituentWorkUnitsCount);
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public double getTopLevelWorkUnitsMedianSize() {
    return this.topLevelQuantilesMinSizes.get(this.quantilesCount / 2);
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public double getConstituentWorkUnitsMedianSize() {
    return this.topLevelQuantilesMinSizes.get(this.quantilesCount / 2);
  }
}
