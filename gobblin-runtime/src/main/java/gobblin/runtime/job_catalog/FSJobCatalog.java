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
package gobblin.runtime.job_catalog;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.util.FSJobCatalogHelper;
import gobblin.util.filesystem.PathAlterationObserver;


/**
 * The job Catalog for file system to persist the job configuration information.
 * This implementation has no support for caching.
 */
public class FSJobCatalog extends ImmutableFSJobCatalog implements MutableJobCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalog.class);
  public static final String CONF_EXTENSION = ".conf";

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param sysConfig
   * @throws Exception
   */
  public FSJobCatalog(Config sysConfig)
      throws Exception {
    super(sysConfig);
  }

  /**
   * The expose of observer is used for testing purpose, so that
   * the checkAndNotify method can be revoked manually, instead of waiting for
   * the scheduling timing.
   * @param sysConfig The same as general constructor.
   * @param observer The user-initialized observer.
   * @throws Exception
   */
  @VisibleForTesting
  protected FSJobCatalog(Config sysConfig, PathAlterationObserver observer)
      throws Exception {
    super(sysConfig, observer);
  }

  /**
   * Allow user to programmatically add a new JobSpec.
   * The method will materialized the jobSpec into real file.
   *
   * @param jobSpec The target JobSpec Object to be materialized.
   *                Noted that the URI return by getUri is a relative path.
   */
  @Override
  public synchronized void put(JobSpec jobSpec) {
    Preconditions.checkNotNull(jobSpec);
    try {
      Path jobSpecPath = getPathForURI(this.jobConfDirPath, jobSpec.getUri());
      FSJobCatalogHelper.materializedJobSpec(jobSpecPath, jobSpec, this.fs);
    } catch (IOException e) {
      throw new RuntimeException("When persisting a new JobSpec, unexpected issues happen:" + e.getMessage());
    } catch (JobSpecNotFoundException e) {
      throw new RuntimeException("When replacing a existed JobSpec, unexpected issue happen:" + e.getMessage());
    }
  }

  /**
   * Allow user to programmatically delete a new JobSpec.
   * This method is designed to be reentrant.
   * @param jobURI The relative Path that specified by user, need to make it into complete path.
   */
  @Override
  public synchronized void remove(URI jobURI) {
    try {
      Path jobSpecPath = getPathForURI(this.jobConfDirPath, jobURI);

      if (fs.exists(jobSpecPath)) {
        fs.delete(jobSpecPath, false);
      } else {
        LOGGER.warn("No file with URI:" + jobSpecPath + " is found. Deletion failed.");
      }
    } catch (IOException e) {
      throw new RuntimeException("When removing a JobConf. file, issues unexpected happen:" + e.getMessage());
    }
  }

  /**
   * It is InMemoryJobCatalog's responsibility to inform the gobblin instance driver about the file change.
   * Here it is internal detector's responsibility.
   */
  @Override
  public boolean shouldLoadGlobalConf() {
    return false;
  }

  @Override
  public Path getPathForURI(Path jobConfDirPath, URI uri) {
    return super.getPathForURI(jobConfDirPath, uri).suffix(CONF_EXTENSION);
  }

  @Override
  protected Optional<String> getInjectedExtension() {
    return Optional.of(CONF_EXTENSION);
  }
}
