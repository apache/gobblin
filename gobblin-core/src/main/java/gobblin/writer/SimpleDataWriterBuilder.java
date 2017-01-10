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


/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes bytes.
 *
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriterBuilder extends FsDataWriterBuilder<String, byte[]> {
  /**
   * Build a {@link gobblin.writer.DataWriter}.
   *
   * @return the built {@link gobblin.writer.DataWriter}
   * @throws java.io.IOException if there is anything wrong building the writer
   */
  @Override
  public DataWriter<byte[]> build() throws IOException {
    return new SimpleDataWriter(this, this.destination.getProperties());
  }

}
