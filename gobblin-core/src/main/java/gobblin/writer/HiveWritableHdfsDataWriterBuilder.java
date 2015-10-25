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

package gobblin.writer;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.configuration.State;
import gobblin.serde.HiveSerDeWrapper;
import gobblin.util.WriterUtils;


/**
 * A {@link DataWriterBuilder} for building {@link HiveWritableHdfsDataWriter}.
 *
 * If properties {@link #WRITER_WRITABLE_CLASS}, {@link #WRITER_OUTPUT_FORMAT_CLASS} and
 * {@link #WRITER_OUTPUT_FILE_EXTENSION} are all specified, their values will be used to create
 * {@link HiveWritableHdfsDataWriter}. Otherwise, property {@link HiveSerDeWrapper#SERDE_SERIALIZER_TYPE} is required,
 * which will be used to create a {@link HiveSerDeWrapper} that contains the information needed to create
 * {@link HiveWritableHdfsDataWriter}.
 *
 * @author ziliu
 */
public class HiveWritableHdfsDataWriterBuilder extends DataWriterBuilder<Object, Writable> {

  public static final String WRITER_WRITABLE_CLASS = "writer.writable.class";
  public static final String WRITER_OUTPUT_FORMAT_CLASS = "writer.output.format.class";
  public static final String WRITER_OUTPUT_FILE_EXTENSION = "writer.output.file.extension";

  @SuppressWarnings("deprecation")
  @Override
  public DataWriter<Writable> build() throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));

    State properties = this.destination.getProperties();

    if (!properties.contains(WRITER_WRITABLE_CLASS) || !properties.contains(WRITER_OUTPUT_FORMAT_CLASS)
        || !properties.contains(WRITER_OUTPUT_FILE_EXTENSION)) {
      HiveSerDeWrapper serializer = HiveSerDeWrapper.getSerializer(properties);
      properties.setProp(WRITER_WRITABLE_CLASS, serializer.getSerDe().getSerializedClass().getName());
      properties.setProp(WRITER_OUTPUT_FORMAT_CLASS, serializer.getOutputFormatClassName());
      properties.setProp(WRITER_OUTPUT_FILE_EXTENSION, serializer.getExtension());
    }

    String fileName = WriterUtils.getWriterFileName(properties, this.branches, this.branch, this.writerId,
        properties.getProp(WRITER_OUTPUT_FILE_EXTENSION));

    return new HiveWritableHdfsDataWriter(properties, fileName, this.branches, this.branch);

  }
}
