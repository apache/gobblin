package gobblin.util;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;


/**
 * Utility class for collecting metadata specific to the current Hadoop cluster.
 *
 * @see {@link ClustersNames}
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
