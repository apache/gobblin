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

package org.apache.gobblin.compaction.mapreduce.avro;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import java.util.Comparator;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;


/**
 * Reducer class for compaction MR job for Avro data.
 *
 * If there are multiple values of the same key, it keeps the last value read.
 *
 * @author Ziyang Liu
 */
public class AvroKeyDedupReducer extends RecordKeyDedupReducerBase<AvroKey<GenericRecord>, AvroValue<GenericRecord>,
    AvroKey<GenericRecord>, NullWritable> {

  public static final String DELTA_SCHEMA_PROVIDER =
      "org.apache.gobblin.compaction." + AvroKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";

  @Override
  protected void initReusableObject() {
    outKey = new AvroKey<>();
    outValue = NullWritable.get();
  }

  @Override
  protected void setOutKey(AvroValue<GenericRecord> valueToRetain) {
    outKey.datum(valueToRetain.datum());
  }

  @Override
  protected void setOutValue(AvroValue<GenericRecord> valueToRetain) {
    // do nothing since initReusableObject has assigned value for outValue.
  }

  @Override
  protected void initDeltaComparator(Configuration conf) {
    deltaComparatorOptional = Optional.absent();
    String deltaSchemaProviderClassName = conf.get(DELTA_SCHEMA_PROVIDER);
    if (deltaSchemaProviderClassName != null) {
      deltaComparatorOptional = Optional.of(new AvroValueDeltaSchemaComparator(
          GobblinConstructorUtils.invokeConstructor(AvroDeltaFieldNameProvider.class, deltaSchemaProviderClassName,
              conf)));
    }
  }


  @VisibleForTesting
  protected static class AvroValueDeltaSchemaComparator implements Comparator<AvroValue<GenericRecord>> {
    private final AvroDeltaFieldNameProvider deltaSchemaProvider;

    public AvroValueDeltaSchemaComparator(AvroDeltaFieldNameProvider provider) {
      this.deltaSchemaProvider = provider;
    }

    @Override
    public int compare(AvroValue<GenericRecord> o1, AvroValue<GenericRecord> o2) {
      GenericRecord record1 = o1.datum();
      GenericRecord record2 = o2.datum();
      for (String deltaFieldName : this.deltaSchemaProvider.getDeltaFieldNames(record1)) {
        if (record1.get(deltaFieldName).equals(record2.get(deltaFieldName))) {
          continue;
        }
        return ((Comparable) record1.get(deltaFieldName)).compareTo(record2.get(deltaFieldName));
      }

      return 0;
    }
  }
}
