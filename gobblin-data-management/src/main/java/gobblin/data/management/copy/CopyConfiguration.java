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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;

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
   * User supplied directory where files should be published. This value is identical for all datasets in the distcp job.
   */
  private final Path publishDir;
  /**
   * Preserve options passed by the user.
   */
  private final PreserveAttributes preserve;
  /**
   * {@link CopyContext} for this job.
   */
  private final CopyContext copyContext;
  private final Optional<String> targetGroup;
  private final FileSystem targetFs;

  public static class CopyConfigurationBuilder {

    private PreserveAttributes preserve;
    private Optional<String> targetGroup;
    private CopyContext copyContext;
    private Path publishDir;

    public CopyConfigurationBuilder(FileSystem targetFs, Properties properties) {

      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          "Missing property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

      this.targetGroup =
          properties.containsKey(DESTINATION_GROUP_KEY) ? Optional.of(properties.getProperty(DESTINATION_GROUP_KEY))
              : Optional.<String> absent();
      this.preserve = PreserveAttributes.fromMnemonicString(properties.getProperty(PRESERVE_ATTRIBUTES_KEY));
      Path publishDirTmp = new Path(properties.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
      if (!publishDirTmp.isAbsolute()) {
        publishDirTmp = new Path(targetFs.getWorkingDirectory(), publishDirTmp);
      }
      this.publishDir = publishDirTmp;
      this.copyContext = new CopyContext();
      this.targetFs = targetFs;
    }
  }

  public static CopyConfigurationBuilder builder(FileSystem targetFs, Properties properties) {
    return new CopyConfigurationBuilder(targetFs, properties);
  }

}
