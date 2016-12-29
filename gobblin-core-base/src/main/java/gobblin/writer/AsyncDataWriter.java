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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;


/**
 * An interface for implementing Async Writers for Gobblin.
 */
// InterfaceStability.Evolving
public interface AsyncDataWriter<D> extends Closeable {

  /**
   * Asynchronously write a record, execute the callback on success/failure
   * @param record
   * @param callback
   * @return Future that the caller could wait on
   */
  Future<WriteResponse> write(D record, @Nullable WriteCallback callback);

  /**
   * Flushes uncommitted records
   * @throws IOException
   */
  void flush() throws IOException;

}
