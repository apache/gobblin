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

package gobblin.runtime.template;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;

import lombok.Getter;


/**
 * A {@link JobTemplate} using a static {@link Config} as the raw configuration for the template.
 */
public class StaticJobTemplate extends InheritingJobTemplate {

  public static final String SUPER_TEMPLATE_KEY = "gobblin.template.inherit";

  private Config rawConfig;
  private Set<String> requiredAttributes;
  @Getter
  private final URI uri;
  @Getter
  private final String version;
  @Getter
  private final String description;

  public StaticJobTemplate(URI uri, String version, String description, Config config, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException {
    this(uri, version, description, config, getSuperTemplateUris(config), catalog);
  }

  protected StaticJobTemplate(URI uri, String version, String description, Config config, List<URI> superTemplateUris,
      JobCatalogWithTemplates catalog) throws SpecNotFoundException, TemplateException {
    super(superTemplateUris, catalog);
    this.uri = uri;
    this.version = version;
    this.description = description;
    this.rawConfig = config;
    this.requiredAttributes = config.hasPath(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST)
        ? new HashSet<>(Arrays.asList(config.getString(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST).split(",")))
        : Sets.<String>newHashSet();
  }

  private static List<URI> getSuperTemplateUris(Config config) throws TemplateException {
    if (config.hasPath(SUPER_TEMPLATE_KEY)) {
      List<URI> uris = Lists.newArrayList();
      for (String uriString : config.getString(SUPER_TEMPLATE_KEY).split(",")) {
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
    return userConfig.withFallback(this.rawConfig);
  }
}
