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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientCache;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;


/**
 * A DatasetFinder that instantiates multiple DatasetFinders. {@link #findDatasets()} will return a union of all the
 * datasets found by each datasetFinder
 * <p>
 * Subclasses will specify the dataset finder class key name to instantiate. If {@link #datasetFinderClassKey()} is set
 * in jobProps, a single datasetFinder is created. Otherwise {@link #datasetFinderImportedByKey()} is used to find all
 * the importedBy {@link URI}s from gobblin config management. The {@link Config} for each {@link URI} should have a
 * {@link #datasetFinderClassKey()} set.
 * </p>
 *
 */
@Slf4j
public abstract class MultiDatasetFinder implements DatasetsFinder<Dataset> {
  protected abstract String datasetFinderClassKey();

  protected abstract String datasetFinderImportedByKey();

  List<DatasetsFinder<Dataset>> datasetFinders;

  protected final Properties jobProps;
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public MultiDatasetFinder(FileSystem fs, Properties jobProps) {
    this.jobProps = jobProps;
    try {
      this.datasetFinders = Lists.newArrayList();

      if (jobProps.containsKey(datasetFinderClassKey())) {
        try {
          this.datasetFinders.add((DatasetsFinder) ConstructorUtils.invokeConstructor(
              Class.forName(jobProps.getProperty(datasetFinderClassKey())), fs, jobProps));
          log.info(String.format("Instantiated datasetfinder %s ", jobProps.getProperty(datasetFinderClassKey())));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
            | ClassNotFoundException e) {
          log.warn(
              String.format("Retention ignored could not instantiate datasetfinder %s.",
                  jobProps.getProperty(datasetFinderClassKey())), e);
        }
      } else {
        ConfigClient client = ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY);
        Collection<URI> importedBys =
            client.getImportedBy(new URI(jobProps.getProperty(datasetFinderImportedByKey())), false);

        for (URI importedBy : importedBys) {
          Config datasetClassConfig = client.getConfig(importedBy);

          try {
            this.datasetFinders
                .add((DatasetsFinder) ConstructorUtils.invokeConstructor(
                    Class.forName(datasetClassConfig.getString(datasetFinderClassKey())), fs, jobProps,
                    datasetClassConfig));
            log.info(String.format("Instantiated datasetfinder %s for %s.",
                datasetClassConfig.getString(datasetFinderClassKey()), importedBy));
          } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            log.warn(String.format("Retention ignored for %s. Could not instantiate datasetfinder %s.", importedBy,
                datasetClassConfig.getString(datasetFinderClassKey())), e);
          }
        }
      }

    } catch (IllegalArgumentException | VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException
        | ConfigStoreCreationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Dataset> findDatasets() throws IOException {
    List<Dataset> datasets = Lists.newArrayList();
    for (DatasetsFinder<Dataset> df : this.datasetFinders) {
      datasets.addAll(df.findDatasets());
    }
    return datasets;
  }

  @Override
  public Path commonDatasetRoot() {
    throw new UnsupportedOperationException("There is no common dataset root for MultiDatasetFinder");
  }
}
