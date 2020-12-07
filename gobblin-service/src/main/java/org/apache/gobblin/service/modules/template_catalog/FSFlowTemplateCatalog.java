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
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.job_catalog.FSJobCatalog;
import org.apache.gobblin.runtime.template.HOCONInputStreamJobTemplate;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template.HOCONInputStreamFlowTemplate;
import org.apache.gobblin.util.PathUtils;

import static org.apache.gobblin.runtime.AbstractJobLauncher.GOBBLIN_JOB_TEMPLATE_KEY;


/**
 * An implementation of a catalog for {@link FlowTemplate}s. Provides basic API for retrieving a {@link FlowTemplate}
 * from the catalog and for retrieving {@link JobTemplate}s that are part of a {@link FlowTemplate}.
 * The flow and job configuration files are assumed to have the following path structure:
 * <p> /path/to/template/catalog/flowName/flow.conf </p>
 * <p> /path/to/template/catalog/flowName/jobs/job1.(job|template) </p>
 * <p> /path/to/template/catalog/flowName/jobs/job2.(job|template) </p>
 *
 * Avoid confusing with {@link org.apache.gobblin.runtime.spec_catalog.FlowCatalog} which is a catalog for
 * {@link org.apache.gobblin.runtime.api.FlowSpec}.
 *
 * Note that any exceptions thrown here should be propagated into called level for handling, since the handling
 * of exceptions while loading/resolving template is subject to caller logic.
 */
@Alpha
public class FSFlowTemplateCatalog extends FSJobCatalog implements FlowCatalogWithTemplates {
  public static final String JOBS_DIR_NAME = "jobs";
  public static final String FLOW_CONF_FILE_NAME = "flow.conf";
  public static final List<String> JOB_FILE_EXTENSIONS = Arrays.asList(".job", ".template");

  protected static final String FS_SCHEME = "FS";

  /**
   * Initialize the FlowCatalog
   * @param sysConfig that must contain the fully qualified path of the flow template catalog
   * @throws IOException
   */
  public FSFlowTemplateCatalog(Config sysConfig)
      throws IOException {
    super(sysConfig.withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        sysConfig.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)));
  }

  /**
   *
   * @param flowTemplateDirURI URI of the flow template directory
   * @return a {@link FlowTemplate}
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   * @throws IOException
   */
  public FlowTemplate getFlowTemplate(URI flowTemplateDirURI)
      throws SpecNotFoundException, JobTemplate.TemplateException, IOException, URISyntaxException {
    if (!validateTemplateURI(flowTemplateDirURI)) {
      throw new JobTemplate.TemplateException(String.format("The FlowTemplate %s is not valid", flowTemplateDirURI));
    }

    String templateCatalogDir = this.sysConfig.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    // path of uri is location of template file relative to the job configuration root directory
    Path templateDirPath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(flowTemplateDirURI.getPath()));
    Path templateFullPath = PathUtils.mergePaths(templateDirPath, new Path(FLOW_CONF_FILE_NAME));
    FileSystem fs = FileSystem.get(templateFullPath.toUri(), new Configuration());

    try (InputStream is = fs.open(templateFullPath)) {
      return new HOCONInputStreamFlowTemplate(is, flowTemplateDirURI, this);
    }
  }

  /**
   *
   * @param flowTemplateDirURI Relative URI of the flow template directory
   * @return a list of {@link JobTemplate}s for a given flow identified by its {@link URI}.
   * @throws IOException
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   */
  public List<JobTemplate> getJobTemplatesForFlow(URI flowTemplateDirURI)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {

    PathFilter extensionFilter = file -> {
      for (String extension : JOB_FILE_EXTENSIONS) {
        if (file.getName().endsWith(extension)) {
          return true;
        }
      }
      return false;
    };

    if (!validateTemplateURI(flowTemplateDirURI)) {
      throw new JobTemplate.TemplateException(String.format("The FlowTemplate %s is not valid", flowTemplateDirURI));
    }

    List<JobTemplate> jobTemplates = new ArrayList<>();

    String templateCatalogDir = this.sysConfig.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);

    //Flow templates are located under templateCatalogDir/flowEdgeTemplates
    Path flowTemplateDirPath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(flowTemplateDirURI));
    //Job files (with extension .job) are located under templateCatalogDir/flowEdgeTemplates/jobs directory.
    Path jobFilePath = new Path(flowTemplateDirPath, JOBS_DIR_NAME);

    FileSystem fs = FileSystem.get(jobFilePath.toUri(), new Configuration());

    for (FileStatus fileStatus : fs.listStatus(jobFilePath, extensionFilter)) {
      Config jobConfig = loadHoconFileAtPath(fileStatus.getPath());
      //Check if the .job file has an underlying job template
      if (jobConfig.hasPath(GOBBLIN_JOB_TEMPLATE_KEY)) {
        URI jobTemplateRelativeUri = new URI(jobConfig.getString(GOBBLIN_JOB_TEMPLATE_KEY));
        if (!jobTemplateRelativeUri.getScheme().equals(FS_SCHEME)) {
          throw new RuntimeException(
              "Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowTemplateDirURI.getScheme());
        }
        Path fullJobTemplatePath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(jobTemplateRelativeUri));
        jobConfig = jobConfig.withFallback(loadHoconFileAtPath(fullJobTemplatePath));
      }
      jobTemplates.add(new HOCONInputStreamJobTemplate(jobConfig, fileStatus.getPath().toUri(), this));
    }
    return jobTemplates;
  }

  private Config loadHoconFileAtPath(Path filePath)
      throws IOException {
    try (InputStream is = fs.open(filePath)) {
      return ConfigFactory.parseReader(new InputStreamReader(is, Charsets.UTF_8));
    }
  }

  /**
   * Determine if an URI of a jobTemplate or a FlowTemplate is valid.
   * @param flowURI The given job/flow template
   * @return true if the URI is valid.
   */
  private boolean validateTemplateURI(URI flowURI) {
    if (!this.sysConfig.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)) {
      log.error("Missing config " + ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
      return false;
    }
    if (!flowURI.getScheme().equals(FS_SCHEME)) {
      log.error(
          "Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowURI.getScheme());
      return false;
    }

    return true;
  }

  public boolean getAndSetShouldRefreshFlowGraph(boolean value) {
    return false;
  }
}
