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

import com.typesafe.config.ConfigFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import gobblin.util.SchedulerUtils;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.TemplateUtils;


public class SimpleGeneralJobTemplate implements JobTemplate {
  private String templatePath;
  private List<String> _userSpecifiedAttributesList;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);
  private Properties configTemplate = new Properties();

  /**
   * Initilized the template by retriving the specified template file and obtain some special attributes.
   * @param templatePath
   */
  public SimpleGeneralJobTemplate(String templatePath) {
    LOGGER.info("Load the job configuration template : " + this.getClass().getName());
    this.templatePath = templatePath;

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
        Arrays.asList(configTemplate.getProperty(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST).split(","));
  }

  /**
   * Return all the attributes set by template already.
   * @return
   */
  @Override
  public Config getRawTemplateConfig() {
    // Pay attention to
    return ConfigFactory.parseProperties(configTemplate);
  }

  /**
   * Return all the attributes that are left for user to fill in.
   * @return
   */
  @Override
  public List<String> getRequiredConfigList() {
    return this._userSpecifiedAttributesList;
  }

  /**
   * Return the combine configuration of template and user customized attributes.
   * @return
   */
  @Override
  public Config getResolvedConfig(Properties userProps) {
    Config jobPropsWithPotentialTemplate = ConfigFactory.parseProperties(userProps);
    if (userProps.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
      jobPropsWithPotentialTemplate = TemplateUtils.mergeTemplateWithUserCustomizedFile(this.configTemplate, userProps);
    }
    return jobPropsWithPotentialTemplate;
  }
}
