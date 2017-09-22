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

/**
 * Represents a set of functionality a job-creator can ask for. Examples could include
 * encryption, compression, partitioning...
 *
 * Each Capability has a name and then a set of associated configuration properties. An example is
 * the encryption algorithm to use.
 */
@Alpha
public class Capability {
  private final String name;
  private final boolean critical;

  /**
   * Create a new Capability description.
   * @param name Name of the capability
   * @param critical If a capability is marked critical and a job is configured with components that can't satisfy
   *                 the capability, Gobblin will refuse to run the job. If it is not critical then Gobblin will simply
   *                 issue a warning.
   */
  public Capability(String name, boolean critical) {
    this.name = name;
    this.critical = critical;
  }

  public String getName() {
    return name;
  }

  public boolean isCritical() {
    return critical;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Capability that = (Capability) o;

    if (critical != that.critical) {
      return false;
    }
    return name != null ? name.equals(that.name) : that.name == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (critical ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Capability{" + "name='" + name + '\'' + ", critical=" + critical + '}';
  }
}
