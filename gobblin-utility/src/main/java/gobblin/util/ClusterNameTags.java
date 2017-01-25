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

package gobblin.util;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;


/**
 * Utility class for collecting metadata specific to the current Hadoop cluster.
 *
 * @see ClustersNames
 */
public class ClusterNameTags {

  public static final String CLUSTER_IDENTIFIER_TAG_NAME = "clusterIdentifier";

  /**
   * Uses {@link #getClusterNameTags(Configuration)} with default Hadoop {@link Configuration}.
   *
   * @return a {@link Map} of key, value pairs containing the cluster metadata
   */
  public static Map<String, String> getClusterNameTags() {
    return getClusterNameTags(new Configuration());
  }

  /**
   * Gets all useful Hadoop cluster metrics.
   *
   * @param conf a Hadoop {@link Configuration} to collect the metadata from
   *
   * @return a {@link Map} of key, value pairs containing the cluster metadata
   */
  public static Map<String, String> getClusterNameTags(Configuration conf) {
    ImmutableMap.Builder<String, String> tagMap = ImmutableMap.builder();

    String clusterIdentifierTag = ClustersNames.getInstance().getClusterName(conf);
    if (!Strings.isNullOrEmpty(clusterIdentifierTag)) {
      tagMap.put(CLUSTER_IDENTIFIER_TAG_NAME, clusterIdentifierTag);
    }
    return tagMap.build();
  }
}
