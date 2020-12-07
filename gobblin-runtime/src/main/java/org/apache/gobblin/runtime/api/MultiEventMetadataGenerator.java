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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.event.EventName;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.util.ClassAliasResolver;


/**
 * A class to add metadata from multiple {@link EventMetadataGenerator}s.
 * {@link EventMetadataGenerator}s are supposed to be provided by a comma separated string.
 * If multiple {@link EventMetadataGenerator}s add the same metadata, the one that comes later will take precedence.
 */
public class MultiEventMetadataGenerator {
  private final List<EventMetadataGenerator> eventMetadataGenerators = Lists.newArrayList();

  public MultiEventMetadataGenerator(List<String> multiEventMetadataGeneratorList) {
    for (String eventMetadatadataGeneratorClassName : multiEventMetadataGeneratorList) {
      try {
        ClassAliasResolver<EventMetadataGenerator> aliasResolver = new ClassAliasResolver<>(EventMetadataGenerator.class);
        this.eventMetadataGenerators.add(aliasResolver.resolveClass(eventMetadatadataGeneratorClassName).newInstance());
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Could not construct EventMetadataGenerator " + eventMetadatadataGeneratorClassName, e);
      }
    }
  }

  public Map<String, String> getMetadata(JobContext jobContext, EventName eventName) {
    Map<String, String> metadata = Maps.newHashMap();

    for (EventMetadataGenerator eventMetadataGenerator : eventMetadataGenerators) {
      metadata.putAll(eventMetadataGenerator.getMetadata(jobContext, eventName));
    }

    return metadata;
  }
}
