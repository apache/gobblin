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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;


/**
 * A {@link JobTemplate} that supports inheriting other {@link JobTemplate}s.
 */
public abstract class InheritingJobTemplate implements JobTemplate {

  private final List<URI> superTemplateUris;
  private final JobCatalogWithTemplates catalog;
  private List<JobTemplate> superTemplates;
  private boolean resolved;

  public InheritingJobTemplate(List<URI> superTemplateUris, JobCatalogWithTemplates catalog) {
    this.superTemplateUris = superTemplateUris;
    this.catalog = catalog;
    this.resolved = false;
  }

  public InheritingJobTemplate(List<JobTemplate> superTemplates) {
    this.superTemplateUris = Lists.newArrayList();
    this.catalog = null;
    this.superTemplates = superTemplates;
    this.resolved = true;
  }

  /**
   * Resolves a list of {@link URI}s to actual {@link JobTemplate}s. This pattern is necessary to detect loops in
   * inheritance and prevent them from causing a stack overflow.
   */
  private synchronized void ensureTemplatesResolved() throws SpecNotFoundException, TemplateException{
    if (this.resolved) {
      return;
    }
    Map<URI, JobTemplate> loadedTemplates = Maps.newHashMap();
    loadedTemplates.put(getUri(), this);
    resolveTemplates(loadedTemplates);
  }

  private void resolveTemplates(Map<URI, JobTemplate> loadedTemplates) throws SpecNotFoundException, TemplateException {
    if (this.resolved) {
      return;
    }
    this.superTemplates = Lists.newArrayList();
    for (URI uri : this.superTemplateUris) {
      if (!loadedTemplates.containsKey(uri)) {
        JobTemplate newTemplate = this.catalog.getTemplate(uri);
        loadedTemplates.put(uri, newTemplate);
        if (newTemplate instanceof InheritingJobTemplate) {
          ((InheritingJobTemplate) newTemplate).resolveTemplates(loadedTemplates);
        }
      }
      this.superTemplates.add(loadedTemplates.get(uri));
    }
    this.resolved = true;
  }

  public Collection<JobTemplate> getSuperTemplates() throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();
    return ImmutableList.copyOf(this.superTemplates);
  }

  @Override
  public Config getRawTemplateConfig() throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();
    return getRawTemplateConfigHelper(Sets.<JobTemplate>newHashSet());
  }

  private Config getRawTemplateConfigHelper(Set<JobTemplate> alreadyInheritedTemplates)
      throws SpecNotFoundException, TemplateException {
    Config rawTemplate = getLocalRawTemplate();
    for (JobTemplate template : Lists.reverse(this.superTemplates)) {
      if (!alreadyInheritedTemplates.contains(template)) {
        alreadyInheritedTemplates.add(template);
        Config thisFallback = template instanceof InheritingJobTemplate
            ? ((InheritingJobTemplate) template).getRawTemplateConfigHelper(alreadyInheritedTemplates)
            : template.getRawTemplateConfig();
        rawTemplate = rawTemplate.withFallback(thisFallback);
      }
    }
    return rawTemplate;
  }

  protected abstract Config getLocalRawTemplate();

  @Override
  public Collection<String> getRequiredConfigList() throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();
    Set<String> allRequired = getRequiredConfigListHelper(Sets.<JobTemplate>newHashSet());
    final Config rawConfig = getRawTemplateConfig();

    Set<String> filteredRequired = Sets.filter(allRequired, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return !rawConfig.hasPath(input);
      }
    });

    return filteredRequired;
  }

  private Set<String> getRequiredConfigListHelper(Set<JobTemplate> alreadyLoadedTemplates)
      throws SpecNotFoundException, TemplateException {
    Set<String> requiredConfigs = Sets.newHashSet(getLocallyRequiredConfigList());
    for (JobTemplate template : this.superTemplates) {
      if (!alreadyLoadedTemplates.contains(template)) {
        alreadyLoadedTemplates.add(template);
        requiredConfigs.addAll(template instanceof InheritingJobTemplate
            ? ((InheritingJobTemplate) template).getRequiredConfigListHelper(alreadyLoadedTemplates)
            : template.getRequiredConfigList());
      }
    }
    return requiredConfigs;
  }

  protected abstract Collection<String> getLocallyRequiredConfigList();

  @Override
  public Config getResolvedConfig(Config userConfig) throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();
    return getResolvedConfigHelper(userConfig, Sets.<JobTemplate>newHashSet());
  }

  private Config getResolvedConfigHelper(Config userConfig, Set<JobTemplate> alreadyLoadedTemplates)
      throws SpecNotFoundException, TemplateException {
    Config config = getLocallyResolvedConfig(userConfig);
    for (JobTemplate template : Lists.reverse(this.superTemplates)) {
      if (!alreadyLoadedTemplates.contains(template)) {
        alreadyLoadedTemplates.add(template);
        Config fallback = template instanceof InheritingJobTemplate
            ? ((InheritingJobTemplate) template).getResolvedConfigHelper(config, alreadyLoadedTemplates)
            : template.getResolvedConfig(config);
        config = config.withFallback(fallback);
      }
    }
    return config;
  }

  protected abstract Config getLocallyResolvedConfig(Config userConfig) throws TemplateException;

}
