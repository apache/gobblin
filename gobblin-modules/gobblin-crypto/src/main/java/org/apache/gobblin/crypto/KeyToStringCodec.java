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
package gobblin.crypto;

/**
 * A package that can convert a key to and from a string representation
 */
public interface KeyToStringCodec {
  /**
   * Given the string representation of a key, return the raw byte format. Eg given "0102", return
   * new byte[] { 1, 2 }
   */
  byte[] decodeKey(String src);

  /**
   * Encode the raw byte format of a key in an encoded value. Eg take the byte array { 1, 2 }
   * and return the hex string "0102"
   */
  String encodeKey(byte[] key);
}
