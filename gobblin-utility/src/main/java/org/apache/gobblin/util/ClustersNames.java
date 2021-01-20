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

package org.apache.gobblin.util;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Allows conversion of URLs identifying a Hadoop cluster (e.g. resource manager url or
 * a job tracker URL) to a human-readable name.
 *
 * <p>The class will automatically load a resource named {@link #URL_TO_NAME_MAP_RESOURCE_NAME} to
 * get a default mapping. It expects this resource to be in the Java Properties file format. The
 * name of the property is the cluster URL and the value is the human-readable name.
 *
 * <p><b>IMPORTANT:</b> Don't forget to escape colons ":" in the file as those may be interpreted
 * as name/value separators.
 */
public class ClustersNames {

  public static final String URL_TO_NAME_MAP_RESOURCE_NAME = "GobblinClustersNames.properties";
  private static final Logger LOG = LoggerFactory.getLogger(ClustersNames.class);
  private static final Configuration HADOOP_CONFIGURATION = new Configuration();

  private static ClustersNames THE_INSTANCE;

  private Properties urlToNameMap = new Properties();

  protected ClustersNames() {

    try (Closer closer = Closer.create()) {
      InputStream propsInput = closer.register(getClass().getResourceAsStream(URL_TO_NAME_MAP_RESOURCE_NAME));
      if (null == propsInput) {
        propsInput = closer.register(ClassLoader.getSystemResourceAsStream(URL_TO_NAME_MAP_RESOURCE_NAME));
      }
      if (null != propsInput) {
        try {
          this.urlToNameMap.load(propsInput);
          LOG.info("Loaded cluster names map:" + this.urlToNameMap);
        } catch (IOException e) {
          LOG.warn("Unable to load cluster names map: " + e, e);
        }
      } else {
        LOG.info("no default cluster mapping found");
      }
    } catch (IOException e) {
      LOG.warn("unable to close resource input stream for " + URL_TO_NAME_MAP_RESOURCE_NAME + ":" + e, e);
    }
  }

  /**
   * Returns human-readable name of the cluster.
   *
   * Method first checks config for exact cluster url match. If nothing is found,
   * it will also check host:port and just hostname match.
   * If it still could not find a match, hostname from the url will be returned.
   *
   * For incomplete or invalid urls, we'll return a name based on clusterUrl,
   * that will have only alphanumeric characters, dashes, underscores and dots.
   * */
  public String getClusterName(String clusterUrl) {
    if (null == clusterUrl) {
      return null;
    }

    List<String> candidates = generateUrlMatchCandidates(clusterUrl);
    for (String candidate : candidates) {
      String name = this.urlToNameMap.getProperty(candidate);
      if (name != null) {
        return name;
      }
    }

    return candidates.get(candidates.size() - 1);
  }

  public void addClusterMapping(String clusterUrl, String clusterName) {
    Preconditions.checkNotNull(clusterUrl, "cluster URL expected");
    Preconditions.checkNotNull(clusterName, "cluster name expected");
    this.urlToNameMap.put(clusterUrl, clusterName);
  }

  public void addClusterMapping(URL clusterUrl, String clusterName) {
    Preconditions.checkNotNull(clusterUrl, "cluster URL expected");
    Preconditions.checkNotNull(clusterName, "cluster name expected");
    this.urlToNameMap.put(clusterUrl.toString(), clusterName);
  }

  /**
   * @see #getClusterName(String) for logic description.
   */
  private static List<String> generateUrlMatchCandidates(String clusterIdentifier) {
    ArrayList<String> candidates = new ArrayList<>();
    candidates.add(clusterIdentifier);

    try {
      URI uri = new URI(clusterIdentifier.trim());
      if (uri.getHost() != null) {
        if (uri.getPort() != -1) {
          candidates.add(uri.getHost() + ":" + uri.getPort());
        }

        // we prefer a config entry with 'host:port', but if it's missing
        // we'll consider just 'host' config entry
        candidates.add(uri.getHost());
      } else if (uri.getScheme() != null && uri.getPath() != null) {
        // we have a scheme and a path, but not the host name
        // assuming local host
        candidates.add("localhost");
      } else {
        candidates.add(getSafeFallbackName(clusterIdentifier));
      }
    } catch (URISyntaxException e) {
      candidates.add(getSafeFallbackName(clusterIdentifier));
    }

    return candidates;
  }

  private static String getSafeFallbackName(String clusterIdentifier) {
    return clusterIdentifier.replaceAll("[^\\w-\\.]", "_");
  }

  /**
   *
   * Returns the cluster name on which the application is running. Uses default hadoop {@link Configuration} to get the
   * url of the resourceManager or jobtracker. The URL is then translated into a human readable cluster name using
   * {@link #getClusterName(String)}
   *
   * @see #getClusterName(Configuration)
   *
   */
  public String getClusterName() {
    return getClusterName(HADOOP_CONFIGURATION);
  }

  /**
   * Returns the cluster name on which the application is running. Uses Hadoop configuration passed in to get the
   * url of the resourceManager or jobtracker. The URL is then translated into a human readable cluster name using
   * {@link #getClusterName(String)}
   *
   * <p>
   * <b>MapReduce mode</b> Uses the value for "yarn.resourcemanager.address" from {@link Configuration} excluding the
   * port number.
   * </p>
   *
   * <p>
   * <b>Standalone mode (outside of hadoop)</b> Uses the Hostname of {@link InetAddress#getLocalHost()}
   * </p>
   *
   * <p>
   * Use {@link #getClusterName(String)} if you already have the cluster URL
   * </p>
   *
   * @see #getClusterName()
   * @param conf Hadoop configuration to use to get resourceManager or jobTracker URLs
   */
  public String getClusterName(Configuration conf) {
    // ResourceManager address in Hadoop2
    String clusterIdentifier = conf.get("yarn.resourcemanager.address");
    clusterIdentifier = getClusterName(clusterIdentifier);

    // If job is running outside of Hadoop (Standalone) use hostname
    // If clusterIdentifier is localhost or 0.0.0.0 use hostname
    if (clusterIdentifier == null || StringUtils.startsWithIgnoreCase(clusterIdentifier, "localhost")
        || StringUtils.startsWithIgnoreCase(clusterIdentifier, "0.0.0.0")) {
      try {
        clusterIdentifier = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        // Do nothing. Tag will not be generated
      }
    }

    return clusterIdentifier;
  }

  public static ClustersNames getInstance() {
    synchronized (ClustersNames.class) {
      if (null == THE_INSTANCE) {
        THE_INSTANCE = new ClustersNames();
      }
      return THE_INSTANCE;

    }
  }

}
