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

package gobblin.data.management.retention.profile;

import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.data.management.util.PathUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;


/**
 * A configurable {@link gobblin.data.management.retention.dataset.finder.DatasetFinder} that looks for
 * {@link gobblin.data.management.retention.dataset.CleanableDataset}s using a glob pattern.
 */
public abstract class ConfigurableGlobDatasetFinder<T extends Dataset> implements DatasetFinder<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurableGlobDatasetFinder.class);

  public static final String DATASET_PATTERN_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "dataset.pattern";
  public static final String DATASET_BLACKLIST_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "dataset.blacklist";

  private final Path datasetPattern;
  private final Optional<Pattern> blacklist;
  protected final FileSystem fs;
  protected final Properties props;

  public ConfigurableGlobDatasetFinder(FileSystem fs, Properties props) throws IOException {
    for (String property : requiredProperties()) {
      Preconditions.checkArgument(props.containsKey(property));
    }
    if (props.containsKey(DATASET_BLACKLIST_KEY) && !Strings.isNullOrEmpty(props.getProperty(DATASET_BLACKLIST_KEY))) {
      this.blacklist = Optional.of(Pattern.compile(props.getProperty(DATASET_BLACKLIST_KEY)));
    } else {
      this.blacklist = Optional.absent();
    }
    this.datasetPattern = new Path(props.getProperty(DATASET_PATTERN_KEY));
    this.fs = fs;
    this.props = props;
  }

  /**
   * List of required properties for subclasses of this dataset. The constructor will check that the input
   * {@link java.util.Properties} contain all properties returned.
   * @return List of all required property keys in the constructor {@link java.util.Properties}.
   */
  public List<String> requiredProperties() {
    return Lists.newArrayList(DATASET_PATTERN_KEY);
  }

  /**
   * Finds all directories satisfying the input glob pattern, and creates a {@link gobblin.data.management.retention.dataset.CleanableDataset}
   * for each one using {@link #datasetAtPath}.
   * @return List of {@link gobblin.data.management.retention.dataset.CleanableDataset}s in the file system.
   * @throws IOException
   */
  @Override
  public List<T> findDatasets() throws IOException {
    List<T> datasets = Lists.newArrayList();
    for (FileStatus fileStatus : this.fs.globStatus(datasetPattern)) {
      Path pathToMatch = PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath());
      if (this.blacklist.isPresent() && this.blacklist.get().matcher(pathToMatch.toString()).find()) {
        continue;
      }
      LOG.info("Found dataset at " + fileStatus.getPath());
      datasets.add(datasetAtPath(fileStatus.getPath()));
    }
    return datasets;
  }

  /**
   * Creates a {@link gobblin.data.management.retention.dataset.CleanableDataset} from a path. The default implementation
   * creates a {@link gobblin.data.management.retention.dataset.ConfigurableDataset}.
   * @param path {@link org.apache.hadoop.fs.Path} where dataset is located.
   * @return {@link gobblin.data.management.retention.dataset.CleanableDataset} at that path.
   * @throws IOException
   */
  public abstract T datasetAtPath(Path path) throws IOException;
}
