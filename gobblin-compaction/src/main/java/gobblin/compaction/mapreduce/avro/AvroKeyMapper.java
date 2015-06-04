/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.mapreduce.avro;

import gobblin.util.AvroUtils;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper class for compaction MR job for Avro data.
 *
 * For each input Avro record, it emits a key-value pair, where key is the projection of the input record
 * on the attributes on which we de-duplicate, and value is the original record.
 *
 * If the number of reducers is set to 0, then it is an identity mapper.
 *
 * @author ziliu
 */
public class AvroKeyMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Object> {

  private AvroKey<GenericRecord> outKey;
  private AvroValue<GenericRecord> outValue;
  private Schema keySchema;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    keySchema = AvroJob.getMapOutputKeySchema(context.getConfiguration());
    outKey = new AvroKey<GenericRecord>();
    outValue = new AvroValue<GenericRecord>();
  }

  @Override
  protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
      InterruptedException {
    if (context.getNumReduceTasks() == 0) {
      context.write(key, NullWritable.get());
    } else {

      // If there are reducers, mapper output key should be converted to the schema used for deduping, and
      // mapper output value should be the original record.
      outKey.datum(AvroUtils.convertRecordSchema(key.datum(), keySchema));
      outValue.datum(key.datum());
      context.write(outKey, outValue);
    }
  }

}
