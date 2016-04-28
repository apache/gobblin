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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.data.management.version.DatasetVersion;


/**
 * Select the newest k versions of the dataset.
 */
public class NewestKSelectionPolicy implements VersionSelectionPolicy<DatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewestKSelectionPolicy.class);

  /**
   * The number of newest versions to select. Only one of
   * {@link #NEWEST_K_VERSIONS_SELECTED_KEY} and {@link #NEWEST_K_VERSIONS_NOTSELECTED_KEY} can
   * be specified. The default is {@link #NEWEST_K_VERSIONS_SELECTED_KEY} with a value of
   * {@link #VERSIONS_SELECTED_DEFAULT_INT}. Valid values are in the range
   * [{@link #MIN_VERSIONS_ALLOWED}, {@link #MAX_VERSIONS_ALLOWED}].
   */
  public static final String NEWEST_K_VERSIONS_SELECTED_KEY = "selection.newestK.versionsSelected";

  /**
   * The number of newest versions to exclude from the result. Only one of
   * {@link #NEWEST_K_VERSIONS_SELECTED_KEY} and {@link #NEWEST_K_VERSIONS_NOTSELECTED_KEY} can
   * be specified. The default is {@link #NEWEST_K_VERSIONS_SELECTED_KEY} with a value of
   * {@link #VERSIONS_SELECTED_DEFAULT}. Valid values are in the range
   * [{@link #MIN_VERSIONS_ALLOWED}, {@link #MAX_VERSIONS_ALLOWED}].
   */
  public static final String NEWEST_K_VERSIONS_NOTSELECTED_KEY = "selection.newestK.versionsNotSelected";

  public static final Integer VERSIONS_SELECTED_DEFAULT = 2;

  public static final Integer MAX_VERSIONS_ALLOWED = 1000000;
  public static final Integer MIN_VERSIONS_ALLOWED = 1;

  /** Number of versions to keep if non-negative; number of versions to exclude if negative. */
  private final int versionsSelected;

  public NewestKSelectionPolicy(int versionsRetained) {
    this.versionsSelected = versionsRetained;
    LOGGER.info(String.format("Will %s %d versions of each dataset.",
        (this.versionsSelected >= 0 ? "select" : "exclude"),
        this.versionsSelected));
  }

  public NewestKSelectionPolicy(Properties props) {
    this(getNumVersionsFromProps(props));

  }

  public NewestKSelectionPolicy(Config config) {
    this(getNumVersionsFromConfig(config));
  }

  private static int validateNumVersions(int numVersions) {
    Preconditions.checkArgument(numVersions >= MIN_VERSIONS_ALLOWED &&
                                numVersions <= MAX_VERSIONS_ALLOWED);
    return numVersions;
  }

  private static int getNumVersionsFromProps(Properties props) {
    if (props.containsKey(NEWEST_K_VERSIONS_SELECTED_KEY)) {
      if (props.containsKey(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
        throw new RuntimeException("Only one of " + NEWEST_K_VERSIONS_SELECTED_KEY +
                                   " and " + NEWEST_K_VERSIONS_NOTSELECTED_KEY +
                                   " can be specified.");
      }
      return validateNumVersions(Integer.parseInt(props.getProperty(NEWEST_K_VERSIONS_SELECTED_KEY)));
    } else if (props.containsKey(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
      return -validateNumVersions(Integer.parseInt(props.getProperty(NEWEST_K_VERSIONS_NOTSELECTED_KEY)));
    } else {
      return VERSIONS_SELECTED_DEFAULT;
    }
  }

  private static int getNumVersionsFromConfig(Config config) {
    if (config.hasPath(NEWEST_K_VERSIONS_SELECTED_KEY)) {
      if (config.hasPath(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
        throw new RuntimeException("Only one of " + NEWEST_K_VERSIONS_SELECTED_KEY +
                                   " and " + NEWEST_K_VERSIONS_NOTSELECTED_KEY +
                                   " can be specified.");
      }
      return validateNumVersions(config.getInt(NEWEST_K_VERSIONS_SELECTED_KEY));
    } else if (config.hasPath(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
      return -validateNumVersions(config.getInt(NEWEST_K_VERSIONS_NOTSELECTED_KEY));
    } else {
      return VERSIONS_SELECTED_DEFAULT;
    }
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<DatasetVersion> listSelectedVersions(List<DatasetVersion> allVersions) {
    if (this.versionsSelected > 0) {
      return getBoundarySafeSublist(allVersions, 0, this.versionsSelected);
    }
    else {
      return getBoundarySafeSublist(allVersions, - this.versionsSelected, allVersions.size());
    }
  }

  static List<DatasetVersion> getBoundarySafeSublist(List<DatasetVersion> l, int fromIndex,
                                                     int toIndex) {
    fromIndex = Math.min(fromIndex, l.size());
    toIndex = Math.min(toIndex, l.size());

    return l.subList(fromIndex, toIndex);
  }

  @VisibleForTesting
  int getVersionsSelected() {
    return versionsSelected;
  }
}
