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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.gobblin.util.AvroUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * A subclass of {@link org.apache.avro.mapreduce.AvroKeyRecordReader}. The purpose is to add a constructor
 * with signature (CombineFileSplit, TaskAttemptContext, Integer), which is required in order to use
 * {@link org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader}.
 *
 * @author Ziyang Liu
 */
public class AvroKeyCombineFileRecordReader extends AvroKeyRecordReader<GenericRecord> {

  private final CombineFileSplit split;
  private final Integer idx;

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public AvroKeyCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext cx, Integer idx) throws IOException {
      this(split, getSchema(split, cx, idx), idx);
  }

  private AvroKeyCombineFileRecordReader(CombineFileSplit split, Schema inputKeySchema, Integer idx) {
    super(inputKeySchema);
    this.split = split;
    this.idx = idx;
  }

  @Override
  public void initialize(InputSplit unusedSplit, TaskAttemptContext cx) throws IOException, InterruptedException {
    super.initialize(
        new FileSplit(this.split.getPath(this.idx), this.split.getOffset(this.idx), this.split.getLength(this.idx),
            null), cx);
  }

  private static Schema getSchema(CombineFileSplit split, TaskAttemptContext cx, Integer idx) throws IOException {
    Schema schema = AvroJob.getInputKeySchema(cx.getConfiguration());
    if (schema != null) {
      return schema;
    }

    Path path = split.getPath(idx);
    FileSystem fs = path.getFileSystem(cx.getConfiguration());
    return AvroUtils.getSchemaFromDataFile(path, fs);
  }

}
