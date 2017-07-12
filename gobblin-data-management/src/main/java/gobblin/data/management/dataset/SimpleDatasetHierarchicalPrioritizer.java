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

package gobblin.data.management.dataset;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;

import gobblin.annotation.Alias;
import gobblin.configuration.State;
import gobblin.data.management.copy.AllEqualComparator;
import gobblin.dataset.Dataset;
import gobblin.util.request_allocation.Requestor;
import gobblin.util.request_allocation.SimpleHierarchicalPrioritizer;


/**
 * A simple type of {@link SimpleHierarchicalPrioritizer} which prioritize {@link Dataset} based on their tier name.
 *
 *  1-1-1 mapping between {@link Dataset} - {@link SimpleDatasetRequest} - {@link SimpleDatasetRequestor}
 *
 * {@link gobblin.util.request_allocation.HierarchicalAllocator} will use {@link TierComparator} from this class
 * to shuffle {@link SimpleDatasetRequestor}s so that high priority tiers will appear in front of low priority tiers.
 *
 * Usage:
 * {@link #TIER_KEY}.<tier-number>=<whitelist-blacklist-pattern>
 * Example:
 * {@link #TIER_KEY}.0 = pattern_0
 * {@link #TIER_KEY}.1 = pattern_1
 */
@Alias("TieredDatasets")
public class SimpleDatasetHierarchicalPrioritizer extends SimpleHierarchicalPrioritizer<SimpleDatasetRequest>
    implements Serializable {

  public static final String CONFIGURATION_PREFIX = "gobblin.prioritizer.datasetTiering";
  public static final String TIER_KEY = CONFIGURATION_PREFIX + ".tier";
  private static final Pattern TIER_PATTERN = Pattern.compile(TIER_KEY + "\\.([0-9]+)");

  public SimpleDatasetHierarchicalPrioritizer(State state) throws IOException {
    super(SimpleDatasetHierarchicalPrioritizer
        .createRequestorComparator(state), new AllEqualComparator());
  }

  public static Comparator<Requestor<SimpleDatasetRequest>>  createRequestorComparator(State state) throws IOException {
    TreeMap<Integer, Pattern> tiers = Maps.newTreeMap();

    Matcher matcher;
    for (Map.Entry<Object, Object> entry : state.getProperties().entrySet()) {
      if (entry.getKey() instanceof String && entry.getValue() instanceof String
          && (matcher = TIER_PATTERN.matcher((String) entry.getKey())).matches()) {
        int tier = Integer.parseInt(matcher.group(1));
        String regex = (String)entry.getValue();
        tiers.put(tier, Pattern.compile(regex));
      }
    }

    return new SimpleDatasetHierarchicalPrioritizer.TierComparator(tiers);
  }

  @AllArgsConstructor
  private static class TierComparator implements Comparator<Requestor<SimpleDatasetRequest>>, Serializable {
    private final TreeMap<Integer, Pattern> tiersMap;

    @Override
    public int compare(Requestor<SimpleDatasetRequest> o1, Requestor<SimpleDatasetRequest> o2) {
      return Integer.compare(findTier(o1), findTier(o2));
    }

    private int findTier(Requestor<SimpleDatasetRequest> requestor) {

      Dataset dataset = ((SimpleDatasetRequestor) requestor).getDataset();

      for (Map.Entry<Integer, Pattern> tier : tiersMap.entrySet()) {
        Pattern pattern = tier.getValue();
        if (pattern.matcher(dataset.datasetURN()).find()) {
          return tier.getKey();
        }
      }
      return Integer.MAX_VALUE;
    }
  }
}
