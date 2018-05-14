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

package org.apache.gobblin.service.modules.template_catalog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;

import org.apache.gobblin.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.job_catalog.FSJobCatalog;
import org.apache.gobblin.runtime.template.HOCONInputStreamJobTemplate;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template.HOCONInputStreamFlowTemplate;


/**
 * An implementation of a catalog for {@link FlowTemplate}s. Provides basic API for retrieving a {@link FlowTemplate}
 * from the catalog and for retrieving {@link JobTemplate}s that are part of a {@link FlowTemplate}.
 */
@Alpha
public class FSFlowCatalog extends FSJobCatalog implements FlowCatalogWithTemplates {
  public static final String JOB_TEMPLATE_DIR_NAME="jobs";
  protected static final String FS_SCHEME = "FS";

  /**
   * Initialize the FlowCatalog
   * @param sysConfig
   * @throws Exception
   */
  public FSFlowCatalog(Config sysConfig)
      throws IOException {
    super(sysConfig.withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, sysConfig.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)));
  }

  public FlowTemplate getFlowTemplate(URI flowUri) throws SpecNotFoundException, JobTemplate.TemplateException, IOException {
    if (!this.sysConfig.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)) {
      throw new RuntimeException("Missing config " + ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    }
    if (!flowUri.getScheme().equals(FS_SCHEME)) {
      throw new RuntimeException("Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowUri.getScheme());
    }
    String templateCatalogDir = this.sysConfig.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    // path of uri is location of template file relative to the job configuration root directory
    Path templateFullPath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(flowUri.getPath()));
    FileSystem fs = FileSystem.get(templateFullPath.toUri(), new Configuration());

    try (InputStream is = fs.open(templateFullPath)) {
      return new HOCONInputStreamFlowTemplate(is, flowUri, this);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   *
   * @param flowUri
   * @return a list of {@link JobTemplate}s for a given flow identified by its {@link URI}.
   * @throws IOException
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   */
  public List<JobTemplate> getJobTemplatesForFlow(URI flowUri)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException {
    if (!this.sysConfig.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)) {
      throw new RuntimeException("Missing config " + ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    }
    if (!flowUri.getScheme().equals(FS_SCHEME)) {
      throw new RuntimeException("Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowUri.getScheme());
    }
    List<JobTemplate> jobTemplates = new ArrayList<>();

    String templateCatalogDir = this.sysConfig.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    Path templateDirPath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(flowUri));
    Path jobTemplatePath = new Path(templateDirPath, JOB_TEMPLATE_DIR_NAME);
    FileSystem fs = FileSystem.get(jobTemplatePath.toUri(), new Configuration());
    for (FileStatus fileStatus : fs.listStatus(jobTemplatePath)) {
      try (InputStream is = fs.open(fileStatus.getPath())) {
        jobTemplates.add(new HOCONInputStreamJobTemplate(is, fileStatus.getPath().toUri(), this));
      }
    }
    return jobTemplates;
  }
}
