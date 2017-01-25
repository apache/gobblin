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

package gobblin.writer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.configuration.State;
import gobblin.hive.HiveSerDeWrapper;


/**
 * A {@link DataWriterBuilder} for building {@link HiveWritableHdfsDataWriter}.
 *
 * If properties {@link #WRITER_WRITABLE_CLASS}, {@link #WRITER_OUTPUT_FORMAT_CLASS} are both specified, their values
 * will be used to create {@link HiveWritableHdfsDataWriter}. Otherwise, property
 * {@link HiveSerDeWrapper#SERDE_SERIALIZER_TYPE} is required, which will be used to create a
 * {@link HiveSerDeWrapper} that contains the information needed to create {@link HiveWritableHdfsDataWriter}.
 *
 * @author Ziyang Liu
 */
public class HiveWritableHdfsDataWriterBuilder<S> extends FsDataWriterBuilder<S, Writable> {

  public static final String WRITER_WRITABLE_CLASS = "writer.writable.class";
  public static final String WRITER_OUTPUT_FORMAT_CLASS = "writer.output.format.class";

  @SuppressWarnings("deprecation")
  @Override
  public DataWriter<Writable> build() throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));

    State properties = this.destination.getProperties();

    if (!properties.contains(WRITER_WRITABLE_CLASS) || !properties.contains(WRITER_OUTPUT_FORMAT_CLASS)) {
      HiveSerDeWrapper serializer = HiveSerDeWrapper.getSerializer(properties);
      properties.setProp(WRITER_WRITABLE_CLASS, serializer.getSerDe().getSerializedClass().getName());
      properties.setProp(WRITER_OUTPUT_FORMAT_CLASS, serializer.getOutputFormatClassName());
    }

    return new HiveWritableHdfsDataWriter(this, properties);
  }

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return true;
  }
}
