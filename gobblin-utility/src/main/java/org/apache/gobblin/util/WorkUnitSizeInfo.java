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

package org.apache.gobblin.util;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tdunning.math.stats.TDigest;

import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Bare-bones size information about a {@link WorkUnit}, possibly a {@link MultiWorkUnit}, where a constituent work unit is one with no children - a leaf.
 *
 * Measurement currently requires the `WorkUnit` to define {@link ServiceConfigKeys#WORK_UNIT_SIZE}, otherwise sizes will be 0 with merely the count of
 * constituent `WorkUnits`.  For the most part, at present, that key is supplied only by {@link org.apache.gobblin.data.management.copy.CopySource}.
 * Nonetheless, the "contract" for any {@link org.apache.gobblin.source.Source} is both clear and reasonable: just add "size" to your `WorkUnit`s to
 * participate.
 *
 * Some sources might count bytes, others num records, possibly with those size-weighted; and of course not all sources extract a definite
 * amount of data, known up front.  In such cases, the {@link #numConstituents} (aka. parallelism potential) may be most informative.
 */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WorkUnitSizeInfo {
  // NOTE: `@NonNull` to include field in `@RequiredArgsConstructor`, despite - "warning: @NonNull is meaningless on a primitive... @RequiredArgsConstructor"
  @NonNull private int numConstituents;
  @NonNull private long totalSize;
  @NonNull private double medianSize;
  @NonNull private double meanSize;
  @NonNull private double stddevSize;

  /** @return the 'zero' {@link WorkUnitSizeInfo} */
  public static WorkUnitSizeInfo empty() {
    return new WorkUnitSizeInfo(0, 0, 0.0, 0.0, 0.0);
  }

  /**
   * convenience factory to measure a {@link WorkUnit} - preferable to direct ctor call
   * @returns {@link #empty()} when the `WorkUnit` is not measurable by defining {@link ServiceConfigKeys#WORK_UNIT_SIZE}
   */
  public static WorkUnitSizeInfo forWorkUnit(WorkUnit workUnit) {
    // NOTE: redundant `instanceof` merely to appease FindBugs - "Unchecked/unconfirmed cast ..."
    if (!workUnit.isMultiWorkUnit() || !(workUnit instanceof MultiWorkUnit)) {
      long wuSize = workUnit.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0);
      return new WorkUnitSizeInfo(1, wuSize, wuSize, wuSize, 0.0);
    } else {
      // WARNING/TODO: NOT resilient to nested multi-workunits... should it be?
      List<WorkUnit> subWorkUnitsList = ((MultiWorkUnit) workUnit).getWorkUnits();
      if (subWorkUnitsList.isEmpty()) {
        return WorkUnitSizeInfo.empty();
      }
      int n = subWorkUnitsList.size();
      TDigest constituentWorkUnitsDigest = TDigest.createDigest(100);
      AtomicLong totalSize = new AtomicLong(0L);
      List<Long> subWorkUnitSizes = subWorkUnitsList.stream().mapToLong(wu -> wu.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0))
          .boxed().collect(Collectors.toList());
      subWorkUnitSizes.forEach(wuSize -> {
        constituentWorkUnitsDigest.add(wuSize);
        totalSize.addAndGet(wuSize);
      });
      double mean = totalSize.get() * 1.0 / n;
      double variance = subWorkUnitSizes.stream().mapToDouble(wuSize -> {
        double meanDiff = wuSize - mean;
        return meanDiff * meanDiff;
      }).sum() / n;
      double stddev = Math.sqrt(variance);
      double median = constituentWorkUnitsDigest.quantile(0.5);
      return new WorkUnitSizeInfo(n, totalSize.get(), median, mean, stddev);
    }
  }

  /**
   * @return stringified, human-readable prop+value encoding that may be inverted with {@link #decode(String)}
   *
   * NOTE: The resulting encoded form will be between 42 and 117 chars:
   *   - presuming - a 32-bit int (max 10 digits), a 64-bit long (max 19 digits), and a 64-bit double (max 19 digits)
   *   * 86 digits maximum for the values - 1*int + 1*long + 3*double = 10 + 19 + 3*19 = 80
   *   * 11 digits minimum for the values - 1*int + 1*long + 3*double = 1 + 1 + 3*3 = 11
   *   * 22 digits for the names [1+5+6+4+6]
   *   * 9 digits for additional syntax [5+4]
   *   = 117 digits (max)
   *   =  42 digits (min)
   */
  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public String encode() {
    return String.format("n=%d-total=%d-median=%.2f-mean=%.2f-stddev=%.2f",
        numConstituents, totalSize, medianSize, meanSize, stddevSize);
  }

  private static final String DECODING_REGEX = "^n=(\\d+)-total=(\\d+)-median=(\\d+(?:\\.\\d+)?)-mean=(\\d+(?:\\.\\d+)?)-stddev=(\\d+(?:\\.\\d+)?)$";
  private static final Pattern decodingPattern = Pattern.compile(DECODING_REGEX);

  /** @return the parsed size info, when `encoded` is in {@link WorkUnitSizeInfo#encode()}-compatible form; otherwise {@link Optional#empty()} */
  public static Optional<WorkUnitSizeInfo> decode(String encoded) {
    Matcher decoding = decodingPattern.matcher(encoded);
    if (!decoding.matches()) {
      return Optional.empty();
    } else {
      return Optional.of(new WorkUnitSizeInfo(
          Integer.parseInt(decoding.group(1)),
          Long.parseLong(decoding.group(2)),
          Double.parseDouble(decoding.group(3)),
          Double.parseDouble(decoding.group(4)),
          Double.parseDouble(decoding.group(5))
      ));
    }
  }
}
