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
package gobblin.data.management.copy.writer;

import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import java.io.IOException;


/**
 * A {@link DataWriterBuilder} for {@link TarArchiveInputStreamDataWriter}
 */
public class TarArchiveInputStreamDataWriterBuilder extends FileAwareInputStreamDataWriterBuilder {

  @Override
  protected DataWriter<FileAwareInputStream> buildWriter() throws IOException {
    return new TarArchiveInputStreamDataWriter(this.destination.getProperties(), this.branches, this.branch);
  }

}
