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

package gobblin.source.extractor.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


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
 * @author ynli
 */
public abstract class OldApiHadoopTextInputSource<S> extends OldApiHadoopFileInputSource<S, Text, LongWritable, Text> {

  @Override
  protected FileInputFormat<LongWritable, Text> getFileInputFormat(State state, JobConf jobConf) {
    TextInputFormat textInputFormat = ReflectionUtils.newInstance(TextInputFormat.class, jobConf);
    textInputFormat.configure(jobConf);
    return textInputFormat;
  }
}
