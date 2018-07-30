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
import com.typesafe.config.ConfigResolveOptions;

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


/**
 * An implementation of a catalog for {@link FlowTemplate}s. Provides basic API for retrieving a {@link FlowTemplate}
 * from the catalog and for retrieving {@link JobTemplate}s that are part of a {@link FlowTemplate}.
 * The flow and job configuration files are assumed to have the following path structure:
 * <p> /path/to/template/catalog/flowName/flow.conf </p>
 * <p> /path/to/template/catalog/flowName/jobs/job1.(job|template) </p>
 * <p> /path/to/template/catalog/flowName/jobs/job2.(job|template) </p>
 */
@Alpha
public class FSFlowCatalog extends FSJobCatalog implements FlowCatalogWithTemplates {
  public static final String JOBS_DIR_NAME = "jobs";
  public static final String FLOW_CONF_FILE_NAME = "flow.conf";
  public static final List<String> JOB_FILE_EXTENSIONS = Arrays.asList(".job", ".template");
  public static final String JOB_TEMPLATE_KEY = "gobblin.template.uri";

  protected static final String FS_SCHEME = "FS";

  /**
   * Initialize the FlowCatalog
   * @param sysConfig that must contain the fully qualified path of the flow template catalog
   * @throws IOException
   */
  public FSFlowCatalog(Config sysConfig)
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
    if (!this.sysConfig.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)) {
      throw new RuntimeException("Missing config " + ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    }
    if (!flowTemplateDirURI.getScheme().equals(FS_SCHEME)) {
      throw new RuntimeException(
          "Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowTemplateDirURI.getScheme());
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
   * @param flowTemplateDirURI URI of the flow template directory
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

    if (!this.sysConfig.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)) {
      throw new RuntimeException("Missing config " + ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    }
    if (!flowTemplateDirURI.getScheme().equals(FS_SCHEME)) {
      throw new RuntimeException(
          "Expected scheme " + FS_SCHEME + " got unsupported scheme " + flowTemplateDirURI.getScheme());
    }
    List<JobTemplate> jobTemplates = new ArrayList<>();

    String templateCatalogDir = this.sysConfig.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY);
    Path templateDirPath = PathUtils.mergePaths(new Path(templateCatalogDir), new Path(flowTemplateDirURI));
    Path jobTemplatePath = new Path(templateDirPath, JOBS_DIR_NAME);
    FileSystem fs = FileSystem.get(jobTemplatePath.toUri(), new Configuration());

    for (FileStatus fileStatus : fs.listStatus(jobTemplatePath, extensionFilter)) {
      Config templateConfig = loadHoconFileAtPath(fileStatus.getPath(), true);
      if (templateConfig.hasPath(JOB_TEMPLATE_KEY)) {
        URI templateUri = new URI(templateConfig.getString(JOB_TEMPLATE_KEY));
        //Strip out the initial "/"
        URI actualResourceUri = new URI(templateUri.getPath().substring(1));
        Path fullTemplatePath =
            new Path(FSFlowCatalog.class.getClassLoader().getResource(actualResourceUri.getPath()).toURI());
        templateConfig = templateConfig.withFallback(loadHoconFileAtPath(fullTemplatePath, true));
      }
      jobTemplates.add(new HOCONInputStreamJobTemplate(templateConfig, fileStatus.getPath().toUri(), this));
    }
    return jobTemplates;
  }

  private Config loadHoconFileAtPath(Path filePath, boolean allowUnresolved)
      throws IOException {
    ConfigResolveOptions options = ConfigResolveOptions.defaults().setAllowUnresolved(allowUnresolved);
    try (InputStream is = fs.open(filePath)) {
      return ConfigFactory.parseReader(new InputStreamReader(is, Charsets.UTF_8)).resolve(options);
    }
  }
}
