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
package org.apache.gobblin.audit.values.sink;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;


/**
 * An interface for persisting value audits
 */
public interface AuditSink extends Closeable {
  /**
   * Write the <code>record</code> to sink
   *
   * @param record to be written
   * @throws IOException if writing this record failed
   */
  public void write(GenericRecord record) throws IOException;
}
