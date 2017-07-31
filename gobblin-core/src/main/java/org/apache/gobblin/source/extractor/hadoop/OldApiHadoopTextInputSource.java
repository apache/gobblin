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

package org.apache.gobblin.source.extractor.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * An extension to {@link OldApiHadoopFileInputSource} that uses a {@link TextInputFormat}.
 *
 * <p>
 *   A concrete implementation of this class should at least implement the
 *   {@link #getExtractor(WorkUnitState, RecordReader, FileSplit, boolean)} method.
 * </p>
 *
 * @param <S> output schema type
 *
 * @author Yinan Li
 */
public abstract class OldApiHadoopTextInputSource<S> extends OldApiHadoopFileInputSource<S, Text, LongWritable, Text> {

  @Override
  protected FileInputFormat<LongWritable, Text> getFileInputFormat(State state, JobConf jobConf) {
    TextInputFormat textInputFormat = ReflectionUtils.newInstance(TextInputFormat.class, jobConf);
    textInputFormat.configure(jobConf);
    return textInputFormat;
  }
}
