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

package org.apache.gobblin.runtime.api;

/**
 * A Serializer / Deserializer for {@link Spec}.
 */
public interface SpecSerDe {
  /***
   * Serializes the {@link Spec} into byte array.
   * @param spec {@link Spec} to serialize.
   * @return byte array of serialized {@link Spec}.
   */
  public byte[] serialize(Spec spec);

  /***
   * Deserialize byte array into a {@link Spec}.
   * @param spec byte array to deserialize.
   * @return deserialized {@link Spec}.
   */
  public Spec deserialize(byte[] spec);
}