/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

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

  private static ClustersNames THE_INSTANCE;

  private Properties _urlToNameMap = new Properties();

  private ClustersNames() {
    Closer closer = Closer.create();
    try {
      InputStream propsInput =
          closer.register(getClass().getResourceAsStream(URL_TO_NAME_MAP_RESOURCE_NAME));
      if (null == propsInput) {
        propsInput =
            closer.register(ClassLoader.getSystemResourceAsStream(URL_TO_NAME_MAP_RESOURCE_NAME));
      }
      if (null != propsInput) {
        try {
          _urlToNameMap.load(propsInput);
          LOG.info("Loaded cluster names map:" + _urlToNameMap);
        }
        catch (IOException e) {
          LOG.warn("Unable to load cluster names map: " + e, e);
        }
      }
      else {
        LOG.info("no default cluster mapping found");
      }
    }
    finally {
      try{
        closer.close();
      }
      catch (IOException e) {
        LOG.warn("unable to close resource input stream for " + URL_TO_NAME_MAP_RESOURCE_NAME +
            ":" + e, e);
      }
    }
  }

  public String getClusterName(String clusterUrl) {
    if (null == clusterUrl) return null;
    String res = _urlToNameMap.getProperty(clusterUrl);
    return null != res ? res : normalizeClusterUrl(clusterUrl);
  }

  public void addClusterMapping(String clusterUrl, String clusterName) {
    Preconditions.checkNotNull(clusterUrl, "cluster URL expected");
    Preconditions.checkNotNull(clusterName, "cluster name expected");
    _urlToNameMap.put(clusterUrl, clusterName);
  }

  public void addClusterMapping(URL clusterUrl, String clusterName) {
    Preconditions.checkNotNull(clusterUrl, "cluster URL expected");
    Preconditions.checkNotNull(clusterName, "cluster name expected");
    _urlToNameMap.put(clusterUrl.toString(), clusterName);
  }

  // Strip out the port number if it is a valid URI
  private static String normalizeClusterUrl(String clusterIdentifier) {
  try {
    URI uri = new URI(clusterIdentifier.trim());
    // URIs without protocol prefix
    if (! uri.isOpaque() && null != uri.getHost()){
      clusterIdentifier = uri.getHost();
    }
  } catch (URISyntaxException e) {
    //leave ID as is
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
