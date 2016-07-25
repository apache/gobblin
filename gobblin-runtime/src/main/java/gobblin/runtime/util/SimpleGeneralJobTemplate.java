/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.util;

import gobblin.util.TemplateUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.util.SchedulerUtils;
import gobblin.configuration.ConfigurationKeys;


public class SimpleGeneralJobTemplate implements JobTemplate {
  private String templatePath;
  private Properties userProps;
  private List<String> _userSpecifiedAttributesList;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);
  Properties configTemplate = new Properties();

  /**
   * Initilized the template by retriving the specified template file and obtain some special attributes.
   * @param templatePath
   */
  public SimpleGeneralJobTemplate(String templatePath, Properties userProps) {
    LOGGER.info("Load the job configuration template : " + this.getClass().getName());
    this.templatePath = templatePath;
    this.userProps = userProps;

    try {
      InputStream inputStream = getClass().getResourceAsStream(this.templatePath);
      if (inputStream != null) {
        configTemplate.load(inputStream);
      } else {
        throw new FileNotFoundException("Template file " + this.templatePath + " doesn't exist");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failure to loading template files into i/o stream");
    }

    this._userSpecifiedAttributesList =
        Arrays.asList(configTemplate.getProperty(ConfigurationKeys.USER_SPECIFIED_ATTR_LIST).split(","));
  }

  /**
   * Return all the attributes set by template already.
   * @return
   */
  @Override
  public Properties getRawTemplateConfig() {
    return configTemplate;
  }

  /**
   * Return all the attributes that are left for user to fill in.
   * @return
   */
  @Override
  public List<String> getRequiredConfigByUser() {
    return this._userSpecifiedAttributesList;
  }

  /**
   * Return the combine configuration of template and user customized attributes.
   * @return
   */
  @Override
  public Properties getResolvedConfig() {
    Properties jobPropsWithPotentialTemplate = userProps;
    if (userProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
      jobPropsWithPotentialTemplate =
          TemplateUtils.mergeTemplateWithUserCustomizedFile(getRawTemplateConfig(), userProps);
    }
    return jobPropsWithPotentialTemplate;
  }
}
