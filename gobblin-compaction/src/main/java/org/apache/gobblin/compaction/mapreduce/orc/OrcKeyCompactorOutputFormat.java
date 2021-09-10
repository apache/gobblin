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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.apache.orc.mapreduce.OrcOutputFormat;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.mapreduce.CompactorOutputCommitter;
import org.apache.gobblin.writer.GobblinOrcMemoryManager;
import org.apache.gobblin.writer.GobblinOrcWriter;

import static org.apache.gobblin.compaction.mapreduce.CompactorOutputCommitter.COMPACTION_OUTPUT_EXTENSION;


/**
 * Extension of {@link OrcOutputFormat} for customized {@link CompactorOutputCommitter}
 */
@Slf4j
public class OrcKeyCompactorOutputFormat extends OrcOutputFormat {
  private FileOutputCommitter committer = null;

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    if (this.committer == null) {
      this.committer = new CompactorOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }
    return this.committer;
  }

  /**
   * Required for extension since super method hard-coded file extension as ".orc". To keep flexibility
   * of extension name, we made it configuration driven.
   * @param taskAttemptContext The source of configuration that determines the file extension
   * @return The {@link RecordWriter} that write out Orc object.
   * @throws IOException
   */
  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String extension = "." + conf.get(COMPACTION_OUTPUT_EXTENSION, "orc" );

    Path filename = getDefaultWorkFile(taskAttemptContext, extension);
    Writer writer = OrcFile.createWriter(filename,
        org.apache.orc.mapred.OrcOutputFormat.buildOptions(conf).memory(new GobblinOrcMemoryManager(conf)));
    int rowBatchSize = conf.getInt(GobblinOrcWriter.ORC_WRITER_BATCH_SIZE, GobblinOrcWriter.DEFAULT_ORC_WRITER_BATCH_SIZE);
    log.info("Creating OrcMapreduceRecordWriter with row batch size = {}", rowBatchSize);
    return new OrcMapreduceRecordWriter(writer, rowBatchSize);
  }
}
