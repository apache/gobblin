/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


/**
 * Hadoop {@link org.apache.hadoop.mapreduce.OutputFormat} implementation with a custom {@link OutputCommitter}
 */
public class GobblinOutputFormat extends NullOutputFormat<NullWritable, NullWritable> {

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new GobblinOutputCommitter();
  }
}
