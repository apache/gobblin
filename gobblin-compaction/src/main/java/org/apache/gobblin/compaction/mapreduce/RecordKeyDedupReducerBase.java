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

package org.apache.gobblin.compaction.mapreduce;

import com.google.common.base.Optional;

import java.io.IOException;
import java.util.Comparator;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * A base implementation of deduplication reducer that is format-unaware.
 */
public abstract class RecordKeyDedupReducerBase<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO> {
  public enum EVENT_COUNTER {
    MORE_THAN_1, DEDUPED, RECORD_COUNT
  }

  /**
   * In most of cases, one of following will be {@link NullWritable}
   */
  @Getter
  protected KO outKey;

  @Getter
  protected VO outValue;

  protected Optional<Comparator<VI>> deltaComparatorOptional;

  protected abstract void initReusableObject();

  /**
   * Assign output value to reusable object.
   * @param valueToRetain the output value determined after dedup process.
   */
  protected abstract void setOutKey(VI valueToRetain);

  /**
   * Added to avoid loss of flexibility to put output value in key/value.
   * Usually for compaction job, either implement {@link #setOutKey} or this.
   */
  protected abstract void setOutValue(VI valueToRetain);

  protected abstract void initDeltaComparator(Configuration conf);

  @Override
  protected void setup(Context context) {
    initReusableObject();
    initDeltaComparator(context.getConfiguration());
  }

  @Override
  protected void reduce(KI key, Iterable<VI> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    VI valueToRetain = null;

    // Preserve only one values among all duplicates.
    for (VI value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      } else if (deltaComparatorOptional.isPresent()) {
        valueToRetain = deltaComparatorOptional.get().compare(valueToRetain, value) >= 0 ? valueToRetain : value;
      }
      numVals++;
    }

    writeRetainedValue(valueToRetain, context);
    updateCounters(numVals, context);
  }

  protected void writeRetainedValue(VI valueToRetain, Context context)
      throws IOException, InterruptedException {
    setOutKey(valueToRetain);
    setOutValue(valueToRetain);

    // Safety check
    if (outKey == null || outValue == null) {
      throw new IllegalStateException("Either outKey or outValue is not being properly initialized");
    }

    context.write(this.outKey, this.outValue);
  }

  /**
   * Update the MR counter based on input {@param numDuplicates}, which indicates the times of duplication of a
   * record seen in a reducer call.
   */
  protected void updateCounters(int numDuplicates, Context context) {
    if (numDuplicates > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numDuplicates - 1);
    }

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }
}
