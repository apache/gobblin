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

package gobblin.data.management.copy;

import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;


/**
 * Configuration for Gobblin distcp jobs.
 */
@Data
@AllArgsConstructor
@Builder
public class CopyConfiguration {

  public static final String COPY_PREFIX = "gobblin.copy";
  public static final String PRESERVE_ATTRIBUTES_KEY = COPY_PREFIX + ".preserved.attributes";
  public static final String DESTINATION_GROUP_KEY = COPY_PREFIX + ".dataset.destination.group";

  /**
   * Directory where dataset should be replicated. This directory corresponds to the {@link CopyableDataset#datasetRoot}
   * in the new location.
   */
  private final Path targetRoot;

  /**
   * Preserve options passed by the user.
   */
  private final PreserveAttributes preserve;

  /**
   * {@link CopyContext} for this job.
   */
  private final CopyContext copyContext;

  private final Optional<String> targetGroup;

  public static class CopyConfigurationBuilder {

    private PreserveAttributes preserve;
    private Optional<String> targetGroup;
    private CopyContext copyContext;

    public CopyConfigurationBuilder(Properties properties) {
      this.targetGroup =
          properties.containsKey(DESTINATION_GROUP_KEY) ? Optional.of(properties.getProperty(DESTINATION_GROUP_KEY))
              : Optional.<String> absent();
      this.preserve = PreserveAttributes.fromMnemonicString(properties.getProperty(PRESERVE_ATTRIBUTES_KEY));
      this.copyContext = new CopyContext();
    }
  }

  public static CopyConfigurationBuilder builder() {
    return new CopyConfigurationBuilder(new Properties());
  }

  public static CopyConfigurationBuilder builder(Properties properties) {
    return new CopyConfigurationBuilder(properties);
  }

}
