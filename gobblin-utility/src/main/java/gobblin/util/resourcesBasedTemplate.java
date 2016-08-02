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
package gobblin.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;


public class resourcesBasedTemplate implements JobTemplate {
  private String templatePath;
  private Set<String> _userSpecifiedAttributesList;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);
  private Properties configTemplate = new Properties();

  /**
   * Initilized the template by retriving the specified template file and obtain some special attributes.
   * @param templatePath
   */
  public resourcesBasedTemplate(String templatePath) {
    LOGGER.info("Load the job configuration template : " + templatePath);
    this.templatePath = templatePath;

    if (this.templatePath != null && this.templatePath.length() > 0) {
      try (InputStream inputStream = getClass().getResourceAsStream("/" + this.templatePath)) {
        if (inputStream != null) {
          configTemplate.load(inputStream);
        } else {
          throw new FileNotFoundException("Template file " + this.templatePath + " doesn't exist");
        }
      } catch (IOException e) {
        throw new RuntimeException("Failure to loading template files into i/o stream");
      }
    } else {
      throw new RuntimeException("Template Path doesn't exist");
    }

    this._userSpecifiedAttributesList = new HashSet<String>(
        Arrays.asList(configTemplate.getProperty(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST).split(",")));
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
  public Set<String> getRequiredConfigList() {
    return this._userSpecifiedAttributesList;
  }

  /**
   * For backward compatibility
   * @param userProps
   * @return
   */
  public Properties getResolvedConfigAsProperties(Properties userProps) {
    Properties jobPropsWithPotentialTemplate = userProps;
    if (jobPropsWithPotentialTemplate.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
      jobPropsWithPotentialTemplate = TemplateUtils.mergeTemplateWithUserCustomizedFile(this.configTemplate, userProps);
    }
    // Validate that each of required attributes is provided by user-specific configuration file.
    for (String aRequiredAttr : _userSpecifiedAttributesList) {
      if (!jobPropsWithPotentialTemplate.containsKey(aRequiredAttr)) {
        throw new RuntimeException("Required attributes is not provided for resolution");
      }
    }
    return jobPropsWithPotentialTemplate;
  }

  /**
   * Return the combine configuration of template and user customized attributes.
   * Also validate the resolution.
   * @return
   */
  @Override
  public Config getResolvedConfig(Properties userProps) {
    return ConfigFactory.parseProperties(getResolvedConfigAsProperties(userProps));
  }
}
