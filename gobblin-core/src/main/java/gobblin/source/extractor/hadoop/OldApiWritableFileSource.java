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

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.hadoop.mapred.RecordReader;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.serde.HiveSerDeWrapper;
import gobblin.source.workunit.WorkUnit;


/**
 * An extension of {@link OldApiHadoopFileInputSource} for sources in {@link Writable} format using a
 * {@link org.apache.hadoop.mapred.FileInputFormat}.
 *
 * The {@link org.apache.hadoop.mapred.FileInputFormat} can either be specified using
 * {@link HadoopFileInputSource#FILE_INPUT_FORMAT_CLASS_KEY}, or by specifying a deserializer via
 * {@link HiveSerDeWrapper#SERDE_DESERIALIZER_TYPE}.
 *
 * @author ziliu
 */
public class OldApiWritableFileSource extends OldApiHadoopFileInputSource<Object, Writable, Object, Writable> {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    if (!state.contains(HadoopFileInputSource.FILE_INPUT_FORMAT_CLASS_KEY)) {
      state.setProp(HadoopFileInputSource.FILE_INPUT_FORMAT_CLASS_KEY,
          HiveSerDeWrapper.getDeserializer(state).getInputFormatClassName());
    }
    return super.getWorkunits(state);
  }

  @Override
  protected OldApiHadoopFileInputExtractor<Object, Writable, Object, Writable> getExtractor(WorkUnitState workUnitState,
      RecordReader<Object, Writable> recordReader, FileSplit fileSplit, boolean readKeys) {
    return new OldApiWritableFileExtractor(recordReader, readKeys);
  }
}
