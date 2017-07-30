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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import gobblin.runtime.api.JobSpec;
import gobblin.util.PathUtils;
import gobblin.util.PullFileLoader;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;


public class FSPathAlterationListenerAdaptor extends PathAlterationListenerAdaptor {
  private final Path jobConfDirPath;
  private final PullFileLoader loader;
  private final Config sysConfig;
  private final JobCatalogListenersList listeners;
  private final ImmutableFSJobCatalog.JobSpecConverter converter;

  FSPathAlterationListenerAdaptor(Path jobConfDirPath, PullFileLoader loader, Config sysConfig,
      JobCatalogListenersList listeners, ImmutableFSJobCatalog.JobSpecConverter converter) {
    this.jobConfDirPath = jobConfDirPath;
    this.loader = loader;
    this.sysConfig = sysConfig;
    this.listeners = listeners;
    this.converter = converter;
  }

  /**
   * Transform the event triggered by file creation into JobSpec Creation for Driver (One of the JobCatalogListener )
   * Create a new JobSpec object and notify each of member inside JobCatalogListenersList
   * @param rawPath This could be complete path to the newly-created configuration file.
   */
  @Override
  public void onFileCreate(Path rawPath) {
    try {
      JobSpec newJobSpec =
          this.converter.apply(loader.loadPullFile(rawPath, sysConfig, false));
      listeners.onAddJob(newJobSpec);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * For already deleted job configuration file, the only identifier is path
   * it doesn't make sense to loadJobConfig Here.
   * @param rawPath This could be the complete path to the newly-deleted configuration file.
   */
  @Override
  public void onFileDelete(Path rawPath) {
    URI jobSpecUri = this.converter.computeURI(rawPath);
    // TODO: fix version
    listeners.onDeleteJob(jobSpecUri, null);
  }

  @Override
  public void onFileChange(Path rawPath) {
    try {
      JobSpec updatedJobSpec =
          this.converter.apply(loader.loadPullFile(rawPath, sysConfig, false));
      listeners.onUpdateJob(updatedJobSpec);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}