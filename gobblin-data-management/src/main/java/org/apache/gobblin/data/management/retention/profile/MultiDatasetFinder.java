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

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import gobblin.util.reflection.GobblinConstructorUtils;


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
  private static final Splitter TAGS_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

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
          log.info(String.format("Instantiating datasetfinder %s ", jobProps.getProperty(datasetFinderClassKey())));
          this.datasetFinders.add((DatasetsFinder) ConstructorUtils.invokeConstructor(
              Class.forName(jobProps.getProperty(datasetFinderClassKey())), fs, jobProps));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
            | ClassNotFoundException e) {
          log.error(
              String.format("Retention ignored could not instantiate datasetfinder %s.",
                  jobProps.getProperty(datasetFinderClassKey())), e);
          Throwables.propagate(e);
        }
      } else if (jobProps.containsKey(datasetFinderImportedByKey())) {

        log.info("Instatiating dataset finders using tag " + jobProps.getProperty(datasetFinderImportedByKey()));

        ConfigClient client = ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY);
        Collection<URI> importedBys = Lists.newArrayList();

        for (String tag : TAGS_SPLITTER.split(jobProps.getProperty(datasetFinderImportedByKey()))) {
          log.info("Looking for datasets that import tag " + tag);
          importedBys.addAll(client.getImportedBy(new URI(tag), false));
        }

        for (URI importedBy : importedBys) {
          Config datasetClassConfig = client.getConfig(importedBy);

          try {
            this.datasetFinders.add((DatasetsFinder) GobblinConstructorUtils.invokeFirstConstructor(
                Class.forName(datasetClassConfig.getString(datasetFinderClassKey())), ImmutableList.of(fs, jobProps,
                    datasetClassConfig), ImmutableList.of(fs, jobProps)));
            log.info(String.format("Instantiated datasetfinder %s for %s.",
                datasetClassConfig.getString(datasetFinderClassKey()), importedBy));
          } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            log.error(String.format("Retention ignored for %s. Could not instantiate datasetfinder %s.", importedBy,
                datasetClassConfig.getString(datasetFinderClassKey())), e);
            Throwables.propagate(e);
          }
        }
      } else {
        log.warn(String.format(
            "NO DATASET_FINDERS FOUND. Either specify dataset finder class at %s or specify the imported tags at %s",
            datasetFinderClassKey(), datasetFinderImportedByKey()));
      }

    } catch (IllegalArgumentException | VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException
        | ConfigStoreCreationException | URISyntaxException e) {
      Throwables.propagate(e);
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
