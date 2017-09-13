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
package org.apache.gobblin.writer;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import parquet.example.data.Group;
import parquet.schema.MessageType;


public class ParquetDataWriterBuilder extends FsDataWriterBuilder<MessageType, Group> {
  @Override
  public DataWriter<Group> build()
      throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

    switch (this.destination.getType()) {
      case HDFS:
        return new ParquetHdfsDataWriter(this, this.destination.getProperties());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }
}
