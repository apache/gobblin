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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;


/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes in Avro format.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class AvroDataWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

  @Override
  public DataWriter<GenericRecord> build()
      throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.AVRO);

    switch (this.destination.getType()) {
      case HDFS:
        State properties = this.destination.getProperties();

        String filePath = properties
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, this.branch));
        // Add the writer ID to the file name so each writer writes to a different
        // file of the same file group defined by the given file name
        String fileName = String.format("%s.%s.%s", properties
                .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, this.branch),
                    "part"), this.writerId, this.format.getExtension());

        return new AvroHdfsDataWriter(properties, filePath, fileName, this.schema, this.branch);
      case KAFKA:
        return new AvroKafkaDataWriter();
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }
}
