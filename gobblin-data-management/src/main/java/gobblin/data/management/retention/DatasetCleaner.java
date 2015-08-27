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

package gobblin.data.management.retention;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;


/**
 * Finds existing versions of datasets and cleans old or deprecated versions.
 */
public class DatasetCleaner {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "dataset.profile.class";

  private final DatasetFinder datasetFinder;

  public DatasetCleaner(FileSystem fs, Properties props) throws IOException {

    Preconditions.checkArgument(props.containsKey(DATASET_PROFILE_CLASS_KEY));
    try {
      Class<?> datasetFinderClass = Class.forName(props.getProperty(DATASET_PROFILE_CLASS_KEY));
      this.datasetFinder =
          (DatasetFinder) datasetFinderClass.getConstructor(FileSystem.class, Properties.class).newInstance(fs, props);
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

  /**
   * Perform the cleanup of old / deprecated dataset versions.
   * @throws IOException
   */
  public void clean() throws IOException {
    List<Dataset> dataSets = this.datasetFinder.findDatasets();

    for (Dataset dataset : dataSets) {
      dataset.clean();
    }
  }
}
