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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


/**
 * Mapper class for compaction MR job for Avro data.
 *
 * For each input Avro record, it emits a key-value pair, where key is the projection of the input record
 * on the attributes on which we de-duplicate, and value is the original record.
 *
 * If the number of reducers is set to 0, then it is an identity mapper.
 *
 * @author Ziyang Liu
 */
public class AvroKeyMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Object> {

  public enum EVENT_COUNTER {
    RECORD_COUNT
  }

  private AvroKey<GenericRecord> outKey;
  private AvroValue<GenericRecord> outValue;
  private Schema keySchema;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.keySchema = AvroJob.getMapOutputKeySchema(context.getConfiguration());
    this.outKey = new AvroKey<>();
    this.outKey.datum(new GenericData.Record(this.keySchema));
    this.outValue = new AvroValue<>();
  }

  @Override
  protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
      throws IOException, InterruptedException {
    if (context.getNumReduceTasks() == 0) {
      context.write(key, NullWritable.get());
    } else {
      populateComparableKeyRecord(key.datum(), this.outKey.datum());
      this.outValue.datum(key.datum());
      try {
        context.write(this.outKey, this.outValue);
      } catch (AvroRuntimeException e) {
        final Path[] paths = ((CombineFileSplit) context.getInputSplit()).getPaths();
        throw new IOException("Unable to process paths " + StringUtils.join(paths, ','), e);
      }
    }
    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

  /**
   * Populate the target record, based on the field values in the source record.
   * Target record's schema should be a subset of source record's schema.
   * Target record's schema cannot have MAP, ARRAY or ENUM fields, or UNION fields that
   * contain these fields.
   */
  private static void populateComparableKeyRecord(GenericRecord source, GenericRecord target) {
    for (Field field : target.getSchema().getFields()) {
      if (field.schema().getType() == Schema.Type.UNION) {

        // Since a UNION has multiple types, we need to use induce() to get the actual type in the record.
        Object fieldData = source.get(field.name());
        Schema actualFieldSchema = GenericData.get().induce(fieldData);
        if (actualFieldSchema.getType() == Schema.Type.RECORD) {

          // If the actual type is RECORD (which may contain another UNION), we need to recursively
          // populate it.
          for (Schema candidateType : field.schema().getTypes()) {
            if (candidateType.getFullName().equals(actualFieldSchema.getFullName())) {
              GenericRecord record = new GenericData.Record(candidateType);
              target.put(field.name(), record);
              populateComparableKeyRecord((GenericRecord) fieldData, record);
              break;
            }
          }
        } else {
          target.put(field.name(), source.get(field.name()));
        }
      } else if (field.schema().getType() == Schema.Type.RECORD) {
        GenericRecord record = (GenericRecord) target.get(field.name());
        if (record == null) {
          record = new GenericData.Record(field.schema());
          target.put(field.name(), record);
        }
        populateComparableKeyRecord((GenericRecord) source.get(field.name()), record);
      } else {
        target.put(field.name(), source.get(field.name()));
      }
    }
  }

}
