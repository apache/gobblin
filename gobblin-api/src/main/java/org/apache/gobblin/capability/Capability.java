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
package org.apache.gobblin.capability;

import org.apache.gobblin.annotation.Alpha;

import lombok.Data;

/**
 * Represents a set of functionality a job-creator can ask for. Examples could include
 * encryption, compression, partitioning...
 *
 * Each Capability has a name and then a set of associated configuration properties. An example is
 * the encryption algorithm to use.
 */
@Alpha
@Data
public class Capability {
  /**
   * Threadsafe capability.
   */
  public static final Capability THREADSAFE = new Capability("THREADSAFE", false);

  private final String name;
  private final boolean critical;
}
