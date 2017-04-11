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
package gobblin.runtime.job_catalog;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.template.HOCONInputStreamJobTemplate;
import gobblin.util.PathUtils;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import gobblin.metrics.MetricContext;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.util.filesystem.PathAlterationObserver;

/**
 * The job Catalog for file system to persist the job configuration information.
 * This implementation has no support for caching.
 */
public class FSJobCatalog extends ImmutableFSJobCatalog implements MutableJobCatalog, JobCatalogWithTemplates {

  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalog.class);
  public static final String CONF_EXTENSION = ".conf";
  private static final String FS_SCHEME = "FS";

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param sysConfig
   * @throws Exception
   */
  public FSJobCatalog(Config sysConfig)
      throws IOException {
    super(sysConfig);
  }

  public FSJobCatalog(GobblinInstanceEnvironment env) throws IOException {
    super(env);
  }

  public FSJobCatalog(Config sysConfig, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) throws IOException{
    super(sysConfig, null, parentMetricContext, instrumentationEnabled);
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
      throws IOException {
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
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(jobSpec);
    try {
      Path jobSpecPath = getPathForURI(this.jobConfDirPath, jobSpec.getUri());
      materializedJobSpec(jobSpecPath, jobSpec, this.fs);
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
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
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

  /**
   * Used for shadow copying in the process of updating a existing job configuration file,
   * which requires deletion of the pre-existed copy of file and create a new one with the same name.
   * Steps:
   *  Create a new one in /tmp.
   *  Safely deletion of old one.
   *  copy the newly created configuration file to jobConfigDir.
   *  Delete the shadow file.
   */
  synchronized void materializedJobSpec(Path jobSpecPath, JobSpec jobSpec, FileSystem fs)
      throws IOException, JobSpecNotFoundException {
    Path shadowDirectoryPath = new Path("/tmp");
    Path shadowFilePath = new Path(shadowDirectoryPath, UUID.randomUUID().toString());
    /* If previously existed, should delete anyway */
    if (fs.exists(shadowFilePath)) {
      fs.delete(shadowFilePath, false);
    }

    ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
    mapBuilder.put(ImmutableFSJobCatalog.DESCRIPTION_KEY_IN_JOBSPEC, jobSpec.getDescription())
        .put(ImmutableFSJobCatalog.VERSION_KEY_IN_JOBSPEC, jobSpec.getVersion());

    if (jobSpec.getTemplateURI().isPresent()) {
      mapBuilder.put(ConfigurationKeys.JOB_TEMPLATE_PATH, jobSpec.getTemplateURI().get().toString());
    }

    Map<String, String> injectedKeys = mapBuilder.build();
    String renderedConfig = ConfigFactory.parseMap(injectedKeys).withFallback(jobSpec.getConfig())
        .root().render(ConfigRenderOptions.defaults());
    try (DataOutputStream os = fs.create(shadowFilePath);
        Writer writer = new OutputStreamWriter(os, Charsets.UTF_8)) {
      writer.write(renderedConfig);
    }

    /* (Optionally:Delete oldSpec) and copy the new one in. */
    if (fs.exists(jobSpecPath)) {
      if (! fs.delete(jobSpecPath, false)) {
        throw new IOException("Unable to delete existing job file: " + jobSpecPath);
      }
    }
    if (!fs.rename(shadowFilePath, jobSpecPath)) {
      throw new IOException("Unable to rename job file: " + shadowFilePath + " to " + jobSpecPath);
    }
  }

  @Override
  public JobTemplate getTemplate(URI uri)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    if (!uri.getScheme().equals(FS_SCHEME)) {
      throw new RuntimeException("Expected scheme " + FS_SCHEME + " got unsupported scheme " + uri.getScheme());
    }

    // path of uri is location of template file relative to the job configuration root directory
    Path templateFullPath = PathUtils.mergePaths(jobConfDirPath, new Path(uri.getPath()));

    try (InputStream is = fs.open(templateFullPath)) {
      return new HOCONInputStreamJobTemplate(is, uri, this);
    } catch (IOException ioe) {
      throw new SpecNotFoundException(uri, ioe);
    }
  }

  @Override
  public Collection<JobTemplate> getAllTemplates() {
    throw new UnsupportedOperationException();
  }
}

