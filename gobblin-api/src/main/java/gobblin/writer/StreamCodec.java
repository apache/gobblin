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
import java.io.InputStream;
import java.io.OutputStream;

import gobblin.annotation.Alpha;


/**
 * Interface for an object that can encrypt a bytestream.
 */
@Alpha
public interface StreamCodec {
  /**
   * Wrap a bytestream and return a new stream that encodes the bytes written to it and writes
   * the result into origStream
   * @param origStream Stream to wrap
   * @return A wrapped stream for encoding
   */
  OutputStream encodeOutputStream(OutputStream origStream) throws IOException;

  /**
   * Wrap a bytestream and return a new stream that will decode
   * any bytes from origStream when read from.
   * @param origStream Stream to wrap
   * @return A wrapped stream for decoding
   */
  InputStream decodeInputStream(InputStream origStream) throws IOException;

  /**
   * Get tag/file extension associated with encoder. This may be added to
   * the file extension or other metadata by various writers in the system.
   */
  String getTag();
}
