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

package org.apache.gobblin.example.hadoop;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.hadoop.HadoopFileInputExtractor;
import org.apache.gobblin.source.extractor.hadoop.HadoopFileInputSource;
import org.apache.gobblin.source.extractor.hadoop.HadoopTextInputSource;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An implementation of {@link org.apache.gobblin.source.extractor.hadoop.HadoopTextInputSource} for reading
 * data from Hadoop.
 *
 * <p>
 *   This source returns an {@link org.apache.gobblin.example.hadoop.HadoopTextFileInputExtractor} to
 *   pull the data from Hadoop.
 * </p>
 *
 * @author Sudarshan Vasudevan
 */

public class HadoopTextFileSource extends HadoopFileInputSource<String,String,LongWritable,Text> {
  @Override
  protected HadoopFileInputExtractor<String,String,LongWritable,Text> getExtractor(WorkUnitState workUnitState, RecordReader recordReader,
      FileSplit fileSplit, boolean readKeys) {
    return new HadoopTextFileInputExtractor(recordReader, readKeys);
  }
}
