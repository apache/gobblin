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

package gobblin.writer;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Extends the {@link AvroDataWriterBuilder} class, and is used to writer Avro data in a date-partitioned fashion. There
 * is currently only support for writing data to HDFS.
 */
public class AvroTimePartitionedWriterBuilder extends AvroDataWriterBuilder {

  @Override
  public DataWriter<GenericRecord> build() throws IOException {

    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

    switch (this.destination.getType()) {
      case HDFS:
        return new AvroHdfsTimePartitionedWriter(this.destination, this.writerId, this.schema, this.format,
            this.branches, this.branch);
      case KAFKA:
        throw new UnsupportedOperationException("The builder " + this.getClass().getName() + " cannot write to "
            + this.destination.getType());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }
}
