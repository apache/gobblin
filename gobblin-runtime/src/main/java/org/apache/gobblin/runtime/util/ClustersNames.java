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
package gobblin.runtime.util;

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
public class ClustersNames extends gobblin.util.ClustersNames {

  private static ClustersNames THE_INSTANCE;

  public static ClustersNames getInstance() {
    synchronized (ClustersNames.class) {
      if (null == THE_INSTANCE) {
        THE_INSTANCE = new ClustersNames();
      }
      return THE_INSTANCE;

    }
  }

}
