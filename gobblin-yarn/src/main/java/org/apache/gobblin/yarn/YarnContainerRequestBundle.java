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

package org.apache.gobblin.yarn;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The class that represents current Yarn container request that will be used by {link @YarnService}.
 * Yarn container allocation should associate with helix tag, as workflows can have specific helix tag setup
 * and specific resource requirement.
 */
@Slf4j
@Getter
public class YarnContainerRequestBundle {
  int totalContainers;
  private final Map<String, Integer> helixTagContainerCountMap;
  private final Map<String, Resource> helixTagResourceMap;
  private final Map<String, Set<String>> resourceHelixTagMap;

  public YarnContainerRequestBundle() {
    this.totalContainers = 0;
    this.helixTagContainerCountMap = new HashMap<>();
    this.helixTagResourceMap = new HashMap<>();
    this.resourceHelixTagMap = new HashMap<>();
  }

  public void add(String helixTag, int containerCount, Resource resource) {
    helixTagContainerCountMap.put(helixTag, helixTagContainerCountMap.getOrDefault(helixTag, 0) + containerCount);
    if(helixTagResourceMap.containsKey(helixTag)) {
      Resource existedResource = helixTagResourceMap.get(helixTag);
      Preconditions.checkArgument(resource.getMemory() == existedResource.getMemory() &&
              resource.getVirtualCores() == existedResource.getVirtualCores(),
          "Helix tag need to have consistent resource requirement. Tag " + helixTag
              + " has existed resource require " + existedResource.toString() + " and different require " + resource.toString());
    } else {
      helixTagResourceMap.put(helixTag, resource);
      Set<String> tagSet = resourceHelixTagMap.getOrDefault(resource.toString(), new HashSet<>());
      tagSet.add(helixTag);
      resourceHelixTagMap.put(resource.toString(), tagSet);
    }
    totalContainers += containerCount;
  }

  // This method assumes the resource requirement for the helix tag is already stored in the map
  public void add(String helixTag, int containerCount) {
    if (!helixTagContainerCountMap.containsKey(helixTag) && !helixTagResourceMap.containsKey(helixTag)) {
      log.error("Helix tag {} is not present in the request bundle yet, can't process the request to add {} "
          + "container for it without specifying the resource requirement", helixTag, containerCount);
      return;
    }
    helixTagContainerCountMap.put(helixTag, helixTagContainerCountMap.get(helixTag) + containerCount);
    this.totalContainers += containerCount;
  }
}