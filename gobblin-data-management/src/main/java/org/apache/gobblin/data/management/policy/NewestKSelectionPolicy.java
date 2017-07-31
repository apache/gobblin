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

package org.apache.gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import lombok.Data;
import lombok.ToString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.version.DatasetVersion;


/**
 * Select the newest k versions of the dataset.
 */
@ToString
public class NewestKSelectionPolicy<T extends DatasetVersion> implements VersionSelectionPolicy<T> {

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

  @Data
  private static class Params {
    private final int versionsSelected;
    private final boolean excludeMode;

    Params(int versionsSelected, boolean excludeMode) {
      Preconditions.checkArgument(versionsSelected >= MIN_VERSIONS_ALLOWED && versionsSelected <= MAX_VERSIONS_ALLOWED);

      this.versionsSelected = versionsSelected;
      this.excludeMode = excludeMode;
    }

    static Params createFromConfig(Config config) {
      if (config.hasPath(NEWEST_K_VERSIONS_SELECTED_KEY)) {
        if (config.hasPath(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
          throw new RuntimeException("Only one of " + NEWEST_K_VERSIONS_SELECTED_KEY + " and "
              + NEWEST_K_VERSIONS_NOTSELECTED_KEY + " can be specified.");
        }
        return new Params(config.getInt(NEWEST_K_VERSIONS_SELECTED_KEY), false);
      } else if (config.hasPath(NEWEST_K_VERSIONS_NOTSELECTED_KEY)) {
        return new Params(config.getInt(NEWEST_K_VERSIONS_NOTSELECTED_KEY), true);
      } else {
        return new Params(VERSIONS_SELECTED_DEFAULT, false);
      }
    }

    static Params createFromProps(Properties props) {
      return createFromConfig(ConfigFactory.parseProperties(props));
    }
  }

  private final Params params;

  private NewestKSelectionPolicy(Params params) {
    this.params = params;
    LOGGER.info(String.format("Will %s %d versions of each dataset.", (this.params.excludeMode ? "select" : "exclude"),
        this.params.versionsSelected));
  }

  public NewestKSelectionPolicy(int versionsRetained, boolean excludeMode) {
    this(new Params(versionsRetained, excludeMode));
  }

  public NewestKSelectionPolicy(Properties props) {
    this(Params.createFromProps(props));
  }

  public NewestKSelectionPolicy(Config config) {
    this(Params.createFromConfig(config));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<T> listSelectedVersions(List<T> allVersions) {
    if (this.isExcludeMode()) {
      return getBoundarySafeSublist(allVersions, this.getVersionsSelected(), allVersions.size());
    }
    return getBoundarySafeSublist(allVersions, 0, this.getVersionsSelected());
  }

  private List<T> getBoundarySafeSublist(List<T> l, int fromIndex, int toIndex) {
    fromIndex = Math.min(fromIndex, l.size());
    toIndex = Math.min(toIndex, l.size());

    return l.subList(fromIndex, toIndex);
  }

  @VisibleForTesting
  int getVersionsSelected() {
    return this.params.getVersionsSelected();
  }

  @VisibleForTesting
  boolean isExcludeMode() {
    return this.params.isExcludeMode();
  }
}
