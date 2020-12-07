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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;


public class OrcValueCombineFileRecordReader extends OrcMapreduceRecordReader {
  private final CombineFileSplit split;
  private final Integer splitIdx;

  public OrcValueCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx)
      throws IOException {
    this(getRecordReaderFromFile(split, context, idx), getSchema(split, context, idx), split, idx);
  }

  public OrcValueCombineFileRecordReader(RecordReader reader, TypeDescription schema, CombineFileSplit split,
      Integer splitIdx)
      throws IOException {
    super(reader, schema);
    this.split = split;
    this.splitIdx = splitIdx;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    super.initialize(new FileSplit(this.split.getPath(this.splitIdx), this.split.getOffset(this.splitIdx),
        this.split.getLength(this.splitIdx), null), taskAttemptContext);
  }

  private static TypeDescription getSchema(CombineFileSplit split, TaskAttemptContext context, Integer idx)
      throws IOException {
    Path path = split.getPath(idx);
    return OrcUtils.getTypeDescriptionFromFile(context.getConfiguration(), path);
  }

  private static RecordReader getRecordReaderFromFile(CombineFileSplit split, TaskAttemptContext context, Integer idx)
      throws IOException {
    Path path = split.getPath(idx);

    // One should avoid using rows() without passing Reader.Options object as the configuration for RecordReader.
    // Note that it is different from OrcFile Reader that getFileReader returns.
    Reader.Options options = new Reader.Options(context.getConfiguration());
    return OrcUtils.getFileReader(context.getConfiguration(), path)
        .rows(options.range(split.getOffset(idx), split.getLength(idx)));
  }
}
