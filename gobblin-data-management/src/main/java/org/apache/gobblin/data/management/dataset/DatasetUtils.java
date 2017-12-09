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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Lists;

import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.IterableDatasetFinderImpl;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.CopyableFileFilter;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Utilities for datasets.
 */
public class DatasetUtils {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.dataset.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "profile.class";
  private static final String PATH_FILTER_KEY = CONFIGURATION_KEY_PREFIX + "path.filter.class";
  private static final String COPYABLE_FILE_FILTER_KEY = CONFIGURATION_KEY_PREFIX + "copyable.file.filter.class";

  private static final PathFilter ACCEPT_ALL_PATH_FILTER = new PathFilter() {

    @Override
    public boolean accept(Path path) {
      return true;
    }
  };

  private static final CopyableFileFilter ACCEPT_ALL_COPYABLE_FILE_FILTER = new CopyableFileFilter() {
    @Override
    public Collection<CopyableFile> filter(FileSystem sourceFs, FileSystem targetFs,
        Collection<CopyableFile> copyableFiles) {

      return copyableFiles;
    }
  };

  /**
   * Instantiate a {@link DatasetsFinder}. The class of the {@link DatasetsFinder} is read from property
   * {@link #DATASET_PROFILE_CLASS_KEY}.
   *
   * @param props Properties used for building {@link DatasetsFinder}.
   * @param fs {@link FileSystem} where datasets are located.
   * @return A new instance of {@link DatasetsFinder}.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T extends org.apache.gobblin.dataset.Dataset> DatasetsFinder<T> instantiateDatasetFinder(Properties props,
      FileSystem fs, String default_class, Object... additionalArgs)
      throws IOException {
    String className = default_class;
    if (props.containsKey(DATASET_PROFILE_CLASS_KEY)) {
      className = props.getProperty(DATASET_PROFILE_CLASS_KEY);
    }
    try {
      Class<?> datasetFinderClass = Class.forName(className);
      List<Object> args = Lists.newArrayList(fs, props);
      if (additionalArgs != null) {
        args.addAll(Lists.newArrayList(additionalArgs));
      }
      return (DatasetsFinder<T>) GobblinConstructorUtils.invokeLongestConstructor(datasetFinderClass, args.toArray());
    } catch (ReflectiveOperationException exception) {
      throw new IOException(exception);
    }
  }

  public static <T extends org.apache.gobblin.dataset.Dataset> IterableDatasetFinder<T> instantiateIterableDatasetFinder(
      Properties props, FileSystem fs, String default_class, Object... additionalArgs) throws IOException {
    DatasetsFinder<T> datasetsFinder = instantiateDatasetFinder(props, fs, default_class, additionalArgs);
    return datasetsFinder instanceof IterableDatasetFinder ? (IterableDatasetFinder<T>) datasetsFinder
        : new IterableDatasetFinderImpl<>(datasetsFinder);
  }

  /**
   * Instantiate a {@link PathFilter} from the class name at key {@link #PATH_FILTER_KEY} in props passed. If key
   * {@link #PATH_FILTER_KEY} is not set, a default {@link #ACCEPT_ALL_PATH_FILTER} is returned
   *
   * @param props that contain path filter classname at {@link #PATH_FILTER_KEY}
   * @return a new instance of {@link PathFilter}. If not key is found, returns an {@link #ACCEPT_ALL_PATH_FILTER}
   */
  public static PathFilter instantiatePathFilter(Properties props) {

    if (!props.containsKey(PATH_FILTER_KEY)) {
      return ACCEPT_ALL_PATH_FILTER;
    }

    try {
      Class<?> pathFilterClass = Class.forName(props.getProperty(PATH_FILTER_KEY));
      return (PathFilter) pathFilterClass.newInstance();
    } catch (ClassNotFoundException exception) {
      throw new RuntimeException(exception);
    } catch (InstantiationException exception) {
      throw new RuntimeException(exception);
    } catch (IllegalAccessException exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Instantiate a {@link CopyableFileFilter} from the class name at key {@link #COPYABLE_FILE_FILTER_KEY} in props
   * passed. If key {@link #COPYABLE_FILE_FILTER_KEY} is not set, a default {@link #ACCEPT_ALL_COPYABLE_FILE_FILTER} is
   * returned
   *
   * @param props that contain path filter classname at {@link #COPYABLE_FILE_FILTER_KEY}
   * @return a new instance of {@link PathFilter}. If not key is found, returns an
   *         {@link #ACCEPT_ALL_COPYABLE_FILE_FILTER}
   */
  public static CopyableFileFilter instantiateCopyableFileFilter(Properties props, Object... additionalArgs) {

    if (!props.containsKey(COPYABLE_FILE_FILTER_KEY)) {
      return ACCEPT_ALL_COPYABLE_FILE_FILTER;
    }

    try {
      Class<?> copyableFileFilterClass = Class.forName(props.getProperty(COPYABLE_FILE_FILTER_KEY));
      return (CopyableFileFilter) GobblinConstructorUtils
          .invokeLongestConstructor(copyableFileFilterClass, additionalArgs);
    } catch (ReflectiveOperationException exception) {
      throw new RuntimeException(exception);
    }
  }
}
