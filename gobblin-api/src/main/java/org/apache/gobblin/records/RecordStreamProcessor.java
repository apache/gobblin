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

package gobblin.records;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;


/**
 * An object that applies some function to a {@link RecordStreamWithMetadata}.
 *
 * For example, converters, quality checkers (filters), etc. are instances of this.
 *
 * @param <SI> input schema type
 * @param <SO> output schema type
 * @param <DI> input data type
 * @param <DO> output data type
 */
public interface RecordStreamProcessor<SI, SO, DI, DO> {

  /**
   * Return a {@link RecordStreamWithMetadata} with the appropriate modifications.
   */
  RecordStreamWithMetadata<DO, SO> processStream(RecordStreamWithMetadata<DI, SI> inputStream, WorkUnitState state)
      throws StreamProcessingException;

  /**
   * Exception allowed by {@link #processStream(RecordStreamWithMetadata, WorkUnitState)}.
   */
  class StreamProcessingException extends IOException {
    public StreamProcessingException(String message) {
      super(message);
    }

    public StreamProcessingException(String message, Throwable cause) {
      super(message, cause);
    }

    public StreamProcessingException(Throwable cause) {
      super(cause);
    }
  }
}
