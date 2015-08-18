/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Reducer class for compaction MR job for Avro data.
 *
 * If there are multiple values of the same key, it keeps the last value read.
 *
 * @author ziliu
 */
public class AvroKeyDedupReducer extends
    Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

  public enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  }

  private AvroKey<GenericRecord> outKey;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    outKey = new AvroKey<GenericRecord>();
  }

  @Override
  protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    for (AvroValue<GenericRecord> value : values) {
      outKey.datum(value.datum());
      numVals++;
    }

    if (numVals > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(outKey, NullWritable.get());
  }
}
