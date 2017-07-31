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

package org.apache.gobblin.util.request_allocation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.data.management.copy.AllEqualComparator;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.data.management.copy.prioritization.FileSetComparator;
import org.apache.gobblin.data.management.partition.CopyableDatasetRequestor;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.Dataset;

import lombok.AllArgsConstructor;


/**
 * A simple {@link FileSetComparator} for {@link HiveDataset}s that allows classifying datasets into tiers, so that lower
 * tiers are higher priority.
 *
 * Usage:
 * {@link #TIER_KEY}.<tier-number>=<whitelist-blacklist-pattern>
 * Example:
 * {@link #TIER_KEY}.0 = importantdb
 * {@link #TIER_KEY}.1 = otherdb,thirddb
 */
@Alias(value = "HiveSimpleTiering")
public class SimpleHiveDatasetTieringPrioritizer extends SimpleHierarchicalPrioritizer<FileSet<CopyEntity>>
    implements FileSetComparator, Serializable {



  public static final String CONFIGURATION_PREFIX = "gobblin.prioritizer.hiveDatasetTiering";
  public static final String TIER_KEY = CONFIGURATION_PREFIX + ".tier";
  private static final Pattern TIER_PATTERN = Pattern.compile(TIER_KEY + "\\.([0-9]+)");

  public SimpleHiveDatasetTieringPrioritizer(Properties properties) throws IOException {
    super(SimpleHiveDatasetTieringPrioritizer.createRequestorComparator(properties), new AllEqualComparator<FileSet<CopyEntity>>());
  }

  private static Comparator<Requestor<FileSet<CopyEntity>>> createRequestorComparator(Properties props)
      throws IOException {
    TreeMap<Integer, WhitelistBlacklist> tiers = Maps.newTreeMap();

    Matcher matcher;
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      if (entry.getKey() instanceof String && entry.getValue() instanceof String
          && (matcher = TIER_PATTERN.matcher((String) entry.getKey())).matches()) {
        int tier = Integer.parseInt(matcher.group(1));
        WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist((String)entry.getValue(), "");

        tiers.put(tier, whitelistBlacklist);
      }
    }

    return new TierComparator(tiers);
  }

  @AllArgsConstructor
  private static class TierComparator implements Comparator<Requestor<FileSet<CopyEntity>>>, Serializable {
    private final TreeMap<Integer, WhitelistBlacklist> tiersMap;

    @Override
    public int compare(Requestor<FileSet<CopyEntity>> o1, Requestor<FileSet<CopyEntity>> o2) {
      return Integer.compare(findTier(o1), findTier(o2));
    }

    private int findTier(Requestor<FileSet<CopyEntity>> requestor) {
      if (!(requestor instanceof CopyableDatasetRequestor)) {
        throw new ClassCastException(String.format("%s can only be used for %s.",
            SimpleHiveDatasetTieringPrioritizer.class.getName(), CopyableDatasetRequestor.class.getName()));
      }

      Dataset dataset = ((CopyableDatasetRequestor) requestor).getDataset();

      if (!(dataset instanceof HiveDataset)) {
        throw new ClassCastException(String.format("%s can only be used for %s.",
            SimpleHiveDatasetTieringPrioritizer.class.getName(), HiveDataset.class.getName()));
      }

      HiveDataset hiveDataset = (HiveDataset) dataset;

      for (Map.Entry<Integer, WhitelistBlacklist> tier : tiersMap.entrySet()) {
        WhitelistBlacklist whitelistBlacklist = tier.getValue();
        if (whitelistBlacklist.acceptTable(hiveDataset.getTable().getDbName(), hiveDataset.getTable().getTableName())) {
          return tier.getKey();
        }
      }
      return Integer.MAX_VALUE;
    }
  }
}
