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

package gobblin.data.management.retention.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.version.DatasetVersion;


/**
 * Retains the newest k versions of the dataset.
 */
public class NewestKRetentionPolicy<T extends DatasetVersion> implements RetentionPolicy<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewestKRetentionPolicy.class);

  /**
   * @deprecated use {@link #NEWEST_K_VERSIONS_RETAINED_KEY}
   */
  @Deprecated
  public static final String VERSIONS_RETAINED_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "versions.retained";

  public static final String NEWEST_K_VERSIONS_RETAINED_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "newestK.versions.retained";

  public static final String VERSIONS_RETAINED_DEFAULT = Integer.toString(2);

  private final int versionsRetained;

  public NewestKRetentionPolicy(int versionsRetained) {
    this.versionsRetained = versionsRetained;
    LOGGER.info(String.format("%s will retain %d versions of each dataset.",
        NewestKRetentionPolicy.class.getName(), this.versionsRetained));
  }

  public NewestKRetentionPolicy(Properties props) {
    if (props.containsKey(VERSIONS_RETAINED_KEY)) {
      this.versionsRetained = Integer.parseInt(props.getProperty(VERSIONS_RETAINED_KEY));
    } else if (props.containsKey(NEWEST_K_VERSIONS_RETAINED_KEY)) {
      this.versionsRetained = Integer.parseInt(props.getProperty(NEWEST_K_VERSIONS_RETAINED_KEY));
    } else {
      this.versionsRetained = Integer.parseInt(VERSIONS_RETAINED_DEFAULT);
    }
  }

  public NewestKRetentionPolicy(Config config) {
    this(Integer.parseInt(config.getString(NEWEST_K_VERSIONS_RETAINED_KEY)));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<T> listDeletableVersions(List<T> allVersions) {
    int newerVersions = 0;
    List<T> deletableVersions = Lists.newArrayList();
    for(T datasetVersion : allVersions) {
      if(newerVersions >= this.versionsRetained) {
        deletableVersions.add(datasetVersion);
      }
      newerVersions++;
    }
    return deletableVersions;
  }
}
