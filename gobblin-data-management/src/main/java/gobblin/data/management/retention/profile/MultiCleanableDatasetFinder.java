/*
 * Copyright (C) 2015-2016 LinkedIn Corp. All rights reserved.
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

import gobblin.data.management.retention.DatasetCleaner;

import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;


/**
 * A Clenable DatasetFinder that instantiates multiple DatasetFinders.
 * <p>
 * If {@link #DATASET_FINDER_CLASS_KEY} is set, a single datasetFinder is created.
 * Otherwise {@link #TAGS_TO_IMPORT_KEY} is used to find all the importedBy {@link URI}s from gobblin config management.
 * The {@link Config} for each {@link URI} should have a {@link #DATASET_FINDER_CLASS_KEY} set.
 * </p>
 *
 */
public class MultiCleanableDatasetFinder extends MultiDatasetFinder {

  public static final String TAGS_TO_IMPORT_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "tag";
  public static final String DATASET_FINDER_CLASS_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "dataset.finder.class";
  public static final String DEPRECATED_DATASET_PROFILE_CLASS_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "dataset.profile.class";


  public MultiCleanableDatasetFinder(FileSystem fs, Properties jobProps) {
    super(fs, jobProps);
  }

  @Override
  protected String datasetFinderClassKey() {
    if (super.jobProps.containsKey(DEPRECATED_DATASET_PROFILE_CLASS_KEY)) {
      return DEPRECATED_DATASET_PROFILE_CLASS_KEY;
    }
    return DATASET_FINDER_CLASS_KEY;
  }

  @Override
  protected String datasetFinderImportedByKey() {
    return TAGS_TO_IMPORT_KEY;
  }

}
