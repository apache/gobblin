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

import org.apache.gobblin.compaction.mapreduce.RecordKeyMapperBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;

import lombok.extern.slf4j.Slf4j;


/**
 * To keep consistent with {@link OrcMapreduceRecordReader}'s decision on implementing
 * {@link RecordReader} with {@link NullWritable} as the key and generic type of value, the ORC Mapper will
 * read in the record as the input value.
 */
@Slf4j
public class OrcValueMapper extends RecordKeyMapperBase<NullWritable, OrcStruct, Object, OrcValue> {

  // This key will only be initialized lazily when dedup is enabled.
  private OrcKey outKey;
  private OrcValue outValue;
  private TypeDescription mrOutputSchema;
  private TypeDescription shuffleKeySchema;
  private JobConf jobConf;

  // This is added mostly for debuggability.
  private static int writeCount = 0;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    this.jobConf = new JobConf(context.getConfiguration());
    this.outKey.configure(jobConf);
    this.outValue = new OrcValue();
    this.mrOutputSchema =
        TypeDescription.fromString(context.getConfiguration().get(OrcConf.MAPRED_INPUT_SCHEMA.getAttribute()));
    this.shuffleKeySchema =
        TypeDescription.fromString(context.getConfiguration().get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()));
  }

  @Override
  protected void map(NullWritable key, OrcStruct orcStruct, Context context)
      throws IOException, InterruptedException {

    // Note that outValue.value is being re-used.
    OrcUtils.upConvertOrcStruct(orcStruct, (OrcStruct) outValue.value, mrOutputSchema);
    try {
      if (context.getNumReduceTasks() == 0) {
        context.write(NullWritable.get(), this.outValue);
      } else {
        fillDedupKey((OrcStruct) outKey.key);
        context.write(this.outKey, this.outValue);
      }
    } catch (IOException e) {
      throw new IOException("Failure in write record no." + writeCount, e);
    }
    writeCount += 1;

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

  /**
   * By default, dedup key contains the whole ORC record, except MAP since {@link org.apache.orc.mapred.OrcMap} is
   * an implementation of {@link java.util.TreeMap} which doesn't accept difference of records within the map in comparison.
   */
  protected void fillDedupKey(OrcStruct originalRecord) {
    OrcUtils.upConvertOrcStruct(originalRecord, (OrcStruct) this.outKey.key, this.shuffleKeySchema);
  }
}
