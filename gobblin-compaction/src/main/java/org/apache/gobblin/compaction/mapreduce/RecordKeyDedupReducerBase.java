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
 * @param <K> Type of input key, e.g AvroKey for Avro
 *           The output value will have the same type as the output value
 *           is usually {@link NullWritable}.
 * @param <V> Type of input value.
 */
public abstract class RecordKeyDedupReducerBase<K, V> extends Reducer<K, V, K, NullWritable> {
  public enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  }

  @Getter
  protected K outKey;

  protected Optional<Comparator<V>> deltaComparatorOptional;


  protected abstract void initOutKey();

  /**
   * Assign output value to reusable object.
   * @param valueToRetain the output value determined after dedup process.
   */
  protected abstract void setOutKey(V valueToRetain);

  protected abstract void initDeltaComparator(Configuration conf);


  @Override
  protected void setup(Context context) {
    initOutKey();
    initDeltaComparator(context.getConfiguration());
  }

  @Override
  protected void reduce(K key, Iterable<V> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    V valueToRetain = null;

    for (V value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      } else if (deltaComparatorOptional.isPresent()) {
        valueToRetain = deltaComparatorOptional.get().compare(valueToRetain, value) >= 0 ? valueToRetain : value;
      }
      numVals++;
    }

    setOutKey(valueToRetain);

    if (numVals > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(this.outKey, NullWritable.get());
  }
}
