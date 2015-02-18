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


/**
 * An implementation of {@link DataWriter} that writes to a Kafka topic.
 *
 * @author ynli
 */
class AvroKafkaDataWriter implements DataWriter<GenericRecord> {

  @Override
  public void write(GenericRecord record)
      throws IOException {
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public void commit()
      throws IOException {
  }

  @Override
  public void cleanup()
      throws IOException {
  }

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() {
    return 0;
  }
}
