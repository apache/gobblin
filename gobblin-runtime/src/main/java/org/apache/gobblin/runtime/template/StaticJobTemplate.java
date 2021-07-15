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

package org.apache.gobblin.runtime.template;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SecureJobTemplate;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A {@link JobTemplate} using a static {@link Config} as the raw configuration for the template.
 */
@Slf4j
public class StaticJobTemplate extends InheritingJobTemplate implements SecureJobTemplate {

  public static final String SUPER_TEMPLATE_KEY = "gobblin.template.inherit";
  public static final String IS_SECURE_KEY = "gobblin.template.isSecure";
  public static final String SECURE_OVERRIDABLE_PROPERTIES_KEYS = "gobblin.template.secure.overridableProperties";

  private Config rawConfig;
  private Set<String> requiredAttributes;
  @Getter
  private final URI uri;
  @Getter
  private final String version;
  @Getter
  private final String description;
  @Getter
  private Collection<String> dependencies;

  public StaticJobTemplate(URI uri, String version, String description, Config config, JobCatalogWithTemplates catalog)
      throws TemplateException {
    this(uri, version, description, config, getSuperTemplateUris(config), catalog);
  }

  /** An constructor that materialize multiple templates into a single static template
   * The constructor provided multiple in-memory templates as the input instead of templateURIs
   * */
  public StaticJobTemplate(URI uri, String version, String description, Config config, List<JobTemplate> templates) {
    super(templates, false);
    this.uri = uri;
    this.version = version;
    this.rawConfig = config;
    this.description = description;
  }

  protected StaticJobTemplate(URI uri, String version, String description, Config config, List<URI> superTemplateUris,
      JobCatalogWithTemplates catalog) {
    super(superTemplateUris, catalog);
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.rawConfig = config;
    this.requiredAttributes = config.hasPath(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST) ? new HashSet<>(
        Arrays.asList(config.getString(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST).split(",")))
        : Sets.<String>newHashSet();
    this.dependencies = config.hasPath(ConfigurationKeys.JOB_DEPENDENCIES) ? Arrays
        .asList(config.getString(ConfigurationKeys.JOB_DEPENDENCIES).split(",")) : new ArrayList<>();
  }

  private static List<URI> getSuperTemplateUris(Config config)
      throws TemplateException {
    if (config.hasPath(SUPER_TEMPLATE_KEY)) {
      List<URI> uris = Lists.newArrayList();
      for (String uriString : ConfigUtils.getStringList(config, SUPER_TEMPLATE_KEY)) {
        try {
          uris.add(new URI(uriString));
        } catch (URISyntaxException use) {
          throw new TemplateException("Super template uri is malformed: " + uriString, use);
        }
      }
      return uris;
    } else {
      return Lists.newArrayList();
    }
  }

  @Override
  protected Config getLocalRawTemplate() {
    return this.rawConfig;
  }

  @Override
  protected Collection<String> getLocallyRequiredConfigList() {
    return this.requiredAttributes;
  }

  @Override
  protected Config getLocallyResolvedConfig(Config userConfig) {
    Config filteredUserConfig = SecureJobTemplate.filterUserConfig(this, userConfig, log);
    return filteredUserConfig.withFallback(this.rawConfig);
  }

  @Override
  public boolean isSecure() {
    return ConfigUtils.getBoolean(this.rawConfig, IS_SECURE_KEY, false);
  }

  @Override
  public Collection<String> overridableProperties() {
    return isSecure() ? ConfigUtils.getStringList(this.rawConfig, SECURE_OVERRIDABLE_PROPERTIES_KEYS) : Collections.emptyList();
  }
}
