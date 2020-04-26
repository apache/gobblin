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

package org.apache.gobblin.compaction.mapreduce.orc;

import java.util.Comparator;

import com.google.common.base.Optional;

import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;


public class OrcKeyDedupReducer extends RecordKeyDedupReducerBase<OrcKey, OrcValue, NullWritable, OrcValue> {
  private static final String ORC_DELTA_SCHEMA_PROVIDER =
      "org.apache.gobblin.compaction." + OrcKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";
  private static final String USING_WHOLE_RECORD_FOR_COMPARE = "usingWholeRecordForCompareInReducer";

  @Override
  protected void setOutValue(OrcValue valueToRetain) {
    // Better to copy instead reassigning reference.
    outValue.value = valueToRetain.value;
  }

  @Override
  protected void setOutKey(OrcValue valueToRetain) {
    // do nothing since initReusableObject has assigned value for outKey.
  }

  @Override
  protected void initDeltaComparator(Configuration conf) {
    deltaComparatorOptional = Optional.absent();
    String compareSchemaString = conf.get(ORC_DELTA_SCHEMA_PROVIDER);
    boolean compareWholeRecord = conf.getBoolean(USING_WHOLE_RECORD_FOR_COMPARE, true);
    if (compareSchemaString != null) {
      deltaComparatorOptional =
          Optional.of(new OrcStructComparator(TypeDescription.fromString(compareSchemaString), compareWholeRecord));
    }
  }

  @Override
  protected void initReusableObject() {
    outKey = NullWritable.get();
    outValue = new OrcValue();
  }

  protected static class OrcStructComparator implements Comparator<OrcValue> {
    private final TypeDescription compareSchema;
    private final boolean compareWholeRecord;
    private OrcStruct projectedStruct1;
    private OrcStruct projectedStruct2;

    public OrcStructComparator(TypeDescription compareSchema, boolean compareWholeRecord) {
      this.compareSchema = compareSchema;
      this.compareWholeRecord = compareWholeRecord;
      projectedStruct1 = (OrcStruct) OrcUtils.createValueRecursively(compareSchema);
      projectedStruct2 = (OrcStruct) OrcUtils.createValueRecursively(compareSchema);
    }

    @Override
    public int compare(OrcValue o1, OrcValue o2) {
      // Avoid unnecessary copy of value by calling upConvertOrcStruct
      if (compareWholeRecord) {
        return ((OrcStruct) o1.value).compareTo((OrcStruct) o2.value);
      } else {
        OrcUtils.upConvertOrcStruct((OrcStruct) o1.value, projectedStruct1, compareSchema);
        OrcUtils.upConvertOrcStruct((OrcStruct) o2.value, projectedStruct2, compareSchema);
        return projectedStruct1.compareTo(projectedStruct2);
      }
    }
  }
}
