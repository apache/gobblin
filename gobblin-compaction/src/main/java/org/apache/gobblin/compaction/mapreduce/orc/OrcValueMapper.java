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
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.mapreduce.RecordKeyMapperBase;

import static org.apache.orc.OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA;


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
  private TypeDescription mrInputSchema;
  private TypeDescription shuffleKeySchema;
  private JobConf jobConf;

  // This is added mostly for debuggability.
  private static int writeCount = 0;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    this.jobConf = new JobConf(context.getConfiguration());
    this.outKey = new OrcKey();
    this.outKey.configure(jobConf);
    this.outValue = new OrcValue();
    this.outValue.configure(jobConf);

    // This is the consistent input-schema among all mappers.
    this.mrInputSchema =
        TypeDescription.fromString(context.getConfiguration().get(OrcConf.MAPRED_INPUT_SCHEMA.getAttribute()));
    this.shuffleKeySchema =
        TypeDescription.fromString(context.getConfiguration().get(MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()));
  }

  @Override
  protected void map(NullWritable key, OrcStruct orcStruct, Context context)
      throws IOException, InterruptedException {

    // Up-convert OrcStruct only if schema differs
    if (!orcStruct.getSchema().equals(this.mrInputSchema)) {
      // Note that outValue.value is being re-used.
      log.info("There's a schema difference between output schema and input schema");
      OrcUtils.upConvertOrcStruct(orcStruct, (OrcStruct) outValue.value, mrInputSchema);
    } else {
      this.outValue.value = orcStruct;
    }

    try {
      if (context.getNumReduceTasks() == 0) {
        context.write(NullWritable.get(), this.outValue);
      } else {
        fillDedupKey(orcStruct);
        context.write(this.outKey, this.outValue);
      }
    } catch (Exception e) {
      String inputPathInString = getInputsplitHelper(context);
      throw new RuntimeException("Failure in write record no." + writeCount + " the processing split is:" + inputPathInString, e);
    }
    writeCount += 1;

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

  private String getInputsplitHelper(Context context) {
    try {
      Field mapContextField = WrappedMapper.Context.class.getDeclaredField("mapContext");
      mapContextField.setAccessible(true);
      Path[] inputPaths = ((CombineFileSplit) ((MapContextImpl) mapContextField.get((WrappedMapper.Context) context))
          .getInputSplit()).getPaths();
      return Arrays.toString(inputPaths);
    } catch (NoSuchFieldException | IllegalAccessException ie) {
      throw new RuntimeException(ie);
    }
  }

  /**
   * By default, dedup key contains the whole ORC record, except MAP since {@link org.apache.orc.mapred.OrcMap} is
   * an implementation of {@link java.util.TreeMap} which doesn't accept difference of records within the map in comparison.
   * Note: This method should have no side-effect on input record.
   */
  private void fillDedupKey(OrcStruct originalRecord) {
    if (!originalRecord.getSchema().equals(this.shuffleKeySchema)) {
      OrcUtils.upConvertOrcStruct(originalRecord, (OrcStruct) this.outKey.key, this.shuffleKeySchema);
    } else {
      this.outKey.key = originalRecord;
    }
  }
}
