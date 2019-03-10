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

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcValue;


public class OrcKeyDedupReducer extends Reducer<OrcKey, OrcValue, NullWritable, OrcValue> {

  // Reusable output record object.
  private OrcValue outValue = new OrcValue();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
  }

  @Override
  protected void reduce(OrcKey key, Iterable<OrcValue> values, Context context)
      throws IOException, InterruptedException {
    int numVal = 0;
    OrcValue valueToRetain = null;

    for (OrcValue orcRecord : values) {
      valueToRetain = orcRecord;
      numVal += 1 ;
    }

    outValue = valueToRetain;
    context.write(NullWritable.get(), outValue);
  }
}
