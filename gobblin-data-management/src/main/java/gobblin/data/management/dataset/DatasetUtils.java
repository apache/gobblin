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

package gobblin.data.management.dataset;

import gobblin.data.management.retention.dataset.finder.DatasetFinder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


/**
 * Utilities for datasets.
 */
public class DatasetUtils {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.dataset.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "profile.class";
  private static final String PATH_FILTER = CONFIGURATION_KEY_PREFIX + "path.filter.class";

  /**
   * Instantiate a {@link DatasetFinder}. The class of the {@link DatasetFinder} is read from property
   * {@link #DATASET_PROFILE_CLASS_KEY}.
   *
   * @param props Properties used for building {@link DatasetFinder}.
   * @param fs {@link FileSystem} where datasets are located.
   * @return A new instance of {@link DatasetFinder}.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T extends Dataset> DatasetFinder<T> instantiateDatasetFinder(Properties props, FileSystem fs,
      String def) throws IOException {
    String className = def;
    if (props.containsKey(DATASET_PROFILE_CLASS_KEY)) {
      className = props.getProperty(DATASET_PROFILE_CLASS_KEY);
    }
    try {
      Class<?> datasetFinderClass = Class.forName(className);
      return (DatasetFinder<T>) datasetFinderClass.getConstructor(FileSystem.class, Properties.class).newInstance(fs,
          props);
    } catch (ClassNotFoundException exception) {
      throw new IOException(exception);
    } catch (NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch (InstantiationException exception) {
      throw new IOException(exception);
    } catch (IllegalAccessException exception) {
      throw new IOException(exception);
    } catch (InvocationTargetException exception) {
      throw new IOException(exception);
    }

  }

  public static PathFilter instantiatePathFilter(Properties props) {

    if (!props.containsKey(PATH_FILTER)) {
      return new PathFilter() {

        @Override
        public boolean accept(Path path) {
          return true;
        }
      };
    }

    try {
      Class<?> pathFilterClass = Class.forName(props.getProperty(PATH_FILTER));
      return (PathFilter) pathFilterClass.newInstance();
    } catch (ClassNotFoundException exception) {
      throw new RuntimeException(exception);
    } catch (InstantiationException exception) {
      throw new RuntimeException(exception);
    } catch (IllegalAccessException exception) {
      throw new RuntimeException(exception);
    }
  }
}
