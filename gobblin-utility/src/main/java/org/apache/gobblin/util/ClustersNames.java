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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;


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

  public String getClusterName(String clusterUrl) {
    if (null == clusterUrl)
      return null;
    String res = this.urlToNameMap.getProperty(clusterUrl);
    return null != res ? res : normalizeClusterUrl(clusterUrl);
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

  // Strip out the port number if it is a valid URI
  private static String normalizeClusterUrl(String clusterIdentifier) {
    try {
      URI uri = new URI(clusterIdentifier.trim());
      // URIs without protocol prefix
      if (!uri.isOpaque() && null != uri.getHost()) {
        clusterIdentifier = uri.getHost();
      } else {
        clusterIdentifier = uri.toString().replaceAll("[/:]"," ").trim().replaceAll(" ", "_");
      }
    } catch (URISyntaxException e) {
      //leave ID as is
    }

    return clusterIdentifier;
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
