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

package gobblin.data.management.copy;

import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.copy.prioritization.FileSetComparator;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;
import gobblin.util.reflection.GobblinConstructorUtils;
import gobblin.util.request_allocation.ResourcePool;


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
  public static final String PRIORITIZATION_PREFIX = COPY_PREFIX + ".prioritization";

  public static final String PRIORITIZER_ALIAS_KEY = PRIORITIZATION_PREFIX + ".prioritizerAlias";
  public static final String MAX_COPY_PREFIX = PRIORITIZATION_PREFIX + ".maxCopy";

  public static final String BINPACKING_MAX_PER_BUCKET_PREFIX = COPY_PREFIX + ".binPacking.maxPerBucket";
  public static final String BUFFER_SIZE = COPY_PREFIX + ".bufferSize";

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
  private final Optional<FileSetComparator> prioritizer;
  private final ResourcePool maxToCopy;

  private final Config config;

  public static class CopyConfigurationBuilder {

    private PreserveAttributes preserve;
    private Optional<String> targetGroup;
    private CopyContext copyContext;
    private Path publishDir;

    public CopyConfigurationBuilder(FileSystem targetFs, Properties properties) {

      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          "Missing property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

      this.config = ConfigUtils.propertiesToConfig(properties);

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
      if (properties.containsKey(PRIORITIZER_ALIAS_KEY)) {
        try {
          this.prioritizer = Optional.of(GobblinConstructorUtils.<FileSetComparator>invokeLongestConstructor(
              new ClassAliasResolver(FileSetComparator.class).resolveClass(properties.getProperty(
                  PRIORITIZER_ALIAS_KEY)),
              properties));
        } catch (ReflectiveOperationException roe) {
          throw new RuntimeException("Could not build prioritizer.", roe);
        }
      } else {
        this.prioritizer = Optional.absent();
      }
      this.maxToCopy = CopyResourcePool.fromConfig(ConfigUtils.getConfigOrEmpty(this.config, MAX_COPY_PREFIX));
    }
  }

  public static CopyConfigurationBuilder builder(FileSystem targetFs, Properties properties) {
    return new CopyConfigurationBuilder(targetFs, properties);
  }

  public Config getPrioritizationConfig() {
    return ConfigUtils.getConfigOrEmpty(this.config, PRIORITIZATION_PREFIX);
  }

}
