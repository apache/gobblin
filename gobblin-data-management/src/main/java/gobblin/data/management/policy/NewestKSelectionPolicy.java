/*
* Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/

package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.data.management.version.DatasetVersion;


/**
 * Select the newest k versions of the dataset.
 */
public class NewestKSelectionPolicy<T extends DatasetVersion> implements VersionSelectionPolicy<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewestKSelectionPolicy.class);

  public static final String NEWEST_K_VERSIONS_SELECTED_KEY = "selection.newestK.versionsSelected";

  public static final String VERSIONS_SELECTED_DEFAULT = Integer.toString(2);

  private final int versionsSelected;

  public NewestKSelectionPolicy(int versionsRetained) {
    this.versionsSelected = versionsRetained;
    LOGGER.info(String.format("%s will select %d versions of each dataset.", NewestKSelectionPolicy.class.getName(),
        this.versionsSelected));
  }

  public NewestKSelectionPolicy(Properties props) {
    this.versionsSelected =
        Integer.parseInt(props.getProperty(NEWEST_K_VERSIONS_SELECTED_KEY, VERSIONS_SELECTED_DEFAULT));

  }

  public NewestKSelectionPolicy(Config config) {
    this(Integer.parseInt(config.getString(NEWEST_K_VERSIONS_SELECTED_KEY)));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<T> listSelectedVersions(List<T> allVersions) {
    int newerVersions = 0;
    List<T> selectedVersions = Lists.newArrayList();
    for (T datasetVersion : allVersions) {
      if (newerVersions < this.versionsSelected) {
        selectedVersions.add(datasetVersion);
      } else {
        break;
      }
      newerVersions++;
    }
    return selectedVersions;
  }
}
