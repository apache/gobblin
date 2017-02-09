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
import java.util.Map;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.partitioner.WriterPartitioner;


/**
 * Extract partitioning-related information from taskState
 */
public class PartitioningCapabilityParser extends CapabilityParser {
  PartitioningCapabilityParser() {
    super(ImmutableSet.of(Capability.PARTITIONED_WRITER));
  }

  public static final String PARTITIONING_SCHEMA = "partitionSchema";

  @Override
  public Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch) {
    boolean capabilityExists = false;
    Map<String, Object> properties = null;

    if (taskState.contains(ConfigurationKeys.WRITER_PARTITIONER_CLASS)) {
      capabilityExists = true;

      // This does instantiate a partitioner every time we parse config, but assumption is that partitioner
      // construction is cheap.
      String partitionerClass = taskState.getProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS);
      try {
        WriterPartitioner partitioner = WriterPartitioner.class.cast(
            ConstructorUtils.invokeConstructor(Class.forName(partitionerClass), taskState, numBranches, branch));
        properties = ImmutableMap.<String, Object>of(PARTITIONING_SCHEMA, partitioner.partitionSchema());
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException("Unable to instantiate class " + partitionerClass, e);
      }
    }

    return ImmutableList.of(new CapabilityRecord(Capability.PARTITIONED_WRITER, capabilityExists, properties));
  }

  public static Object getPartitioningSchema(Map<String, Object> properties) {
    return properties.get(PARTITIONING_SCHEMA);
  }

}
