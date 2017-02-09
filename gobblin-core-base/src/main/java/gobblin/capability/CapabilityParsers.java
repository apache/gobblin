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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import gobblin.configuration.State;

import lombok.extern.slf4j.Slf4j;


/**
 * Helper methods for parsing capabilities out of job config
 */
@Slf4j
public class CapabilityParsers {
  private final static Map<Capability, CapabilityParser> parsers;

  /**
   * Return a list of all requested capabilities for a given branch
   * @param taskState Overall task state
   * @param numBranches Number of branches in state
   * @param branch Branch we are interested in
   * @return A map of all capabilities requested for the branch. The value in the map is the associated config
   *         for the capability. If a capability is not requested it will not be returned in the map.
   */
  public static Map<Capability, Map<String, Object>> allWriterCapabilitiesForBranch(State taskState, int numBranches,
      int branch) {

    Map<Capability, Map<String, Object>> capabilities = new HashMap<>();
    for (CapabilityParser parser : parsers.values()) {
      Collection<CapabilityParser.CapabilityRecord> records = parser.parseForBranch(taskState, numBranches, branch);
      if (records.size() != parser.getSupportedCapabilities().size()) {
        log.warn("Expected parser {} to return {} records, actually returned {}", parser.getClass().getCanonicalName(),
            parser.getSupportedCapabilities().size(), records.size());
      }

      for (CapabilityParser.CapabilityRecord record : records) {
        if (record.supportsCapability()) {
          capabilities.put(record.getCapability(), record.getParameters());
        }
      }
    }

    return capabilities;
  }

  /**
   * Return a CapabilityRecord for a specific capability.
   * @param capability Capability we are interested in
   * @param taskState Overall task state
   * @param numBranches Number of branches in state
   * @param branch Branch we are interested in
   * @return A CapabilityRecord indicating whether the capability has been requested for this branch, and, if so,
   *         associated parameters.
   */
  public static CapabilityParser.CapabilityRecord writerCapabilityForBranch(Capability capability, State taskState,
      int numBranches, int branch) {
    CapabilityParser parser = parsers.get(capability);
    if (parser == null) {
      throw new IllegalArgumentException("No parser found for capability " + capability.toString());
    }

    Collection<CapabilityParser.CapabilityRecord> records = parser.parseForBranch(taskState, numBranches, branch);
    for (CapabilityParser.CapabilityRecord record : records) {
      if (record.getCapability().equals(capability)) {
        return record;
      }
    }

    log.warn("Parser {} did not return record for capability {}, assuming it is not present",
        parser.getClass().getCanonicalName(), capability.getName());
    return new CapabilityParser.CapabilityRecord(capability, false, Collections.<String, Object>emptyMap());
  }

  /* TODO: hardcoding logic here means people can't add new capabilities without touching core code;
   * should this be more extensible at some point?
   */
  static {
    parsers = new HashMap<>();
    addParser(new EncryptionCapabilityParser());
    addParser(new PartitioningCapabilityParser());
  }

  private static void addParser(CapabilityParser parser) {
    for (Capability capability : parser.getSupportedCapabilities()) {
      log.debug("Adding capability parser for {}", capability.getName());
      CapabilityParser oldParser = parsers.put(capability, parser);
      if (oldParser != null) {
        log.warn("Duplicate capability parsers for {}, replaced parser {} with {}", capability.getName(),
            oldParser.getClass().getCanonicalName(), parser.getClass().getCanonicalName());
      }
    }
  }

  private CapabilityParsers() {
    // static methods only, can't instantiate
  }
}
