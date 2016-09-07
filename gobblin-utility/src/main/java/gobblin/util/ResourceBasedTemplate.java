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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ResourceBasedTemplate implements JobTemplate {
  private String templatePath;
  private Set<String> _userSpecifiedAttributesList;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);
  private final Config config;

  /**
   * Initilized the template by retrieving the specified template file and obtain some special attributes.
   * @param templatePath
   */
  public ResourceBasedTemplate(String templatePath) throws TemplateException {
    LOGGER.info("Load the job configuration template : " + templatePath);
    this.templatePath = templatePath;

    if (this.templatePath != null && this.templatePath.length() > 0) {
      try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(this.templatePath);
        InputStreamReader reader = new InputStreamReader(inputStream, Charsets.UTF_8)) {
        Config tmpConfig = ConfigFactory.parseReader(reader);
        this._userSpecifiedAttributesList = new HashSet<>(Splitter.on(",").omitEmptyStrings().
            splitToList(tmpConfig.getString(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST)));
        this.config = tmpConfig.root().toConfig();
      } catch (IOException ioe) {
        throw new TemplateException("Failure to loading template files into i/o stream", ioe);
      } catch (NullPointerException npe) {
        throw new TemplateException("Could not find resource with path " + this.templatePath);
      }
    } else {
      throw new TemplateException(String.format("Invalid template path %s", this.templatePath));
    }
  }

  /**
   * Return all the attributes set by template already.
   * @return
   */
  @Override
  public Config getRawTemplateConfig() {
    // Pay attention to
    return this.config;
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
   * Return the combine configuration of template and user customized attributes.
   * Also validate the resolution.
   * @return
   */
  @Override
  public Config getResolvedConfig(Config userProps) throws TemplateException {
    for (String required : this.getRequiredConfigList()) {
      if (!userProps.hasPath(required)) {
        throw new TemplateException(String.format("Missing required property %s for template %s.", required, this.templatePath));
      }
    }
    return userProps.withFallback(this.config).resolve();
  }
}
