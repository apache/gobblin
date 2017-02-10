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
package gobblin.capability;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import gobblin.configuration.State;


/**
 * Represents a class that can parse a set of capabilities out of job/task configuration.
 */
public abstract class CapabilityParser {
  /**
   * Initialize the parser.
   * @param supportedCapabilities Collection of capabilities this parser can extract
   */
  CapabilityParser(Set<Capability> supportedCapabilities) {
    this.supportedCapabilities = supportedCapabilities;
  }

  /**
   * Return a set of CapabilityRecords for a given branch by looking through job config and returning a set of
   * CapabilityRecords for each capability the object knows how to parser.
   *
   * One CapabilityRecord should be returned for each supported capability.
   * @param taskState Overall task state
   * @param numBranches Number of branches in the task
   * @param branch Branch number to retrieve capabilities for
   * @return Collection of capability records showing whether a given capability is enabled for the branch and any associated properties.
   *
   */
  public abstract Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch);

  /**
   * @return Return the capabilities this parser supports
   */
  public Set<Capability> getSupportedCapabilities() {
    return supportedCapabilities;
  }

  /**
   * Describes a Capability with associated configuration parameters
   */
  public static class CapabilityRecord {
    private Capability capability;
    private final boolean supportsCapability;
    private final Map<String, Object> parameters;

    public CapabilityRecord(Capability capability, boolean supportsCapability, Map<String, Object> parameters) {
      this.supportsCapability = supportsCapability;
      this.parameters = parameters;
      this.capability = capability;
    }

    /**
     * Return whether or not the capability is supported
     * @return True if supported, false if not
     */
    public boolean supportsCapability() {
      return supportsCapability;
    }

    /**
     * Synonym for supportsCapability()
     * @return
     */
    public boolean isConfigured() {
      return supportsCapability;
    }

    /**
     * Retrieve any parameters associated with the capability
     * @return
     */
    public Map<String, Object> getParameters() {
      return parameters;
    }

    public Capability getCapability() {
      return capability;
    }

    @Override
    public String toString() {
      return "CapabilityRecord{" + "capability=" + capability + ", supportsCapability=" + supportsCapability + ", parameters="
          + parameters + '}';
    }
  }

  /**
   * Extract a set of properties for a given branch, stripping out the prefix and branch
   * suffix.
   *
   * Eg - original output:
   *  writer.encrypt.1 -> foo
   *  writer.encrypt.something.1 -> bar
   *
   *  will transform to
   *
   *  "" -> foo
   *  something - bar
   *
   * @param properties Properties to extract data from
   * @param prefix Prefix to match; all other properties will be ignored
   * @param numBranches # of branchces
   * @param branch Branch # to extract
   * @return Transformed properties as described above
   */
  protected Map<String, Object> extractPropertiesForBranch(
      Properties properties, String prefix, int numBranches, int branch) {

    Map<String, Object> ret = new HashMap<>();
    String branchSuffix = (numBranches > 1) ? String.format(".%d", branch) : "";

    for (Map.Entry<Object, Object> prop: properties.entrySet()) {
      String key = (String)prop.getKey();
      if (key.startsWith(prefix) && (branchSuffix.length() == 0 || key.endsWith(branchSuffix))) {
        int strippedKeyStart =  Math.min(key.length(), prefix.length() + 1);
        int strippedKeyEnd = Math.max(strippedKeyStart, key.length() - branchSuffix.length());
        String strippedKey = key.substring(strippedKeyStart, strippedKeyEnd);
        ret.put(strippedKey, prop.getValue());
      }
    }

    return ret;
  }

  private final Set<Capability> supportedCapabilities;
}
