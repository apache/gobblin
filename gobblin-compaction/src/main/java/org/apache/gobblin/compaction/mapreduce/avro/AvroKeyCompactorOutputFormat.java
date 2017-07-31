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

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class used with {@link MRCompactorAvroKeyDedupJobRunner} as an entirely normal
 * {@link AvroKeyOutputFormat}, except that the outputted file names contain
 * a timestamp and a count of how many records the file contains in the form:
 * {recordCount}.{timestamp}.avro
 */
public class AvroKeyCompactorOutputFormat<T> extends AvroKeyOutputFormat<T> {

  private FileOutputCommitter committer = null;

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    if (this.committer == null) {
      this.committer = new AvroKeyCompactorOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }
    return this.committer;
  }

}
