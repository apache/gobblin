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

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.util.Comparator;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Reducer class for compaction MR job for Avro data.
 *
 * If there are multiple values of the same key, it keeps the last value read.
 *
 * @author Ziyang Liu
 */
public class AvroKeyDedupReducer extends Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

  public enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  }

  public static final String DELTA_SCHEMA_PROVIDER =
      "gobblin.compaction." + AvroKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";
  private AvroKey<GenericRecord> outKey;
  private Optional<AvroValueDeltaSchemaComparator> deltaComparatorOptional;
  private AvroDeltaFieldNameProvider deltaFieldNamesProvider;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    this.outKey = new AvroKey<>();
    this.deltaComparatorOptional = Optional.absent();
    Configuration conf = context.getConfiguration();
    String deltaSchemaProviderClassName = conf.get(DELTA_SCHEMA_PROVIDER);
    if (deltaSchemaProviderClassName != null) {
      this.deltaFieldNamesProvider =
          GobblinConstructorUtils.invokeConstructor(AvroDeltaFieldNameProvider.class, deltaSchemaProviderClassName, conf);
      this.deltaComparatorOptional = Optional.of(new AvroValueDeltaSchemaComparator(deltaFieldNamesProvider));
    }
  }

  @Override
  protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    AvroValue<GenericRecord> valueToRetain = null;

    for (AvroValue<GenericRecord> value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      } else if (this.deltaComparatorOptional.isPresent()) {
        valueToRetain = this.deltaComparatorOptional.get().compare(valueToRetain, value) >= 0 ? valueToRetain : value;
      }
      numVals++;
    }
    this.outKey.datum(valueToRetain.datum());

    if (numVals > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(this.outKey, NullWritable.get());
  }

  @VisibleForTesting
  protected static class AvroValueDeltaSchemaComparator implements Comparator<AvroValue<GenericRecord>> {
    private final AvroDeltaFieldNameProvider deltaSchemaProvider;

    public AvroValueDeltaSchemaComparator(AvroDeltaFieldNameProvider provider) {
      this.deltaSchemaProvider = provider;
    }

    @Override
    public int compare(AvroValue<GenericRecord> o1, AvroValue<GenericRecord> o2) {
      GenericRecord record1= o1.datum();
      GenericRecord record2 = o2.datum();
      for (String deltaFieldName : this.deltaSchemaProvider.getDeltaFieldNames(record1)) {
        if (record1.get(deltaFieldName).equals(record2.get(deltaFieldName))) {
          continue;
        }
        return ((Comparable)record1.get(deltaFieldName)).compareTo(record2.get(deltaFieldName));
      }

      return 0;
    }
  }

  @VisibleForTesting
  protected AvroKey<GenericRecord> getOutKey() {
    return this.outKey;
  }
}
