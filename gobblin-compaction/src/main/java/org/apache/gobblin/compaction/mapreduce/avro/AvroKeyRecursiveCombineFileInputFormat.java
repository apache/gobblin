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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.gobblin.compaction.mapreduce.CompactionCombineFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


/**
 * A subclass of {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat} for Avro inputfiles.
 * This class is able to handle the case where the input path has subdirs which contain data files, which
 * is not the case with {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}.
 *
 * @author Ziyang Liu
 */
public class AvroKeyRecursiveCombineFileInputFormat
    extends CompactionCombineFileInputFormat<AvroKey<GenericRecord>, NullWritable> {

  @Override
  public RecordReader<AvroKey<GenericRecord>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext cx)
      throws IOException {
    return new CombineFileRecordReader<>((CombineFileSplit) split, cx, AvroKeyCombineFileRecordReader.class);
  }
}
