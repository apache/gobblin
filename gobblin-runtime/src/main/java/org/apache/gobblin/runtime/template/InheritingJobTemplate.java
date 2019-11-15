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

import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;


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

  public InheritingJobTemplate(List<JobTemplate> superTemplates, boolean skipResolve) {
    this.superTemplateUris = Lists.newArrayList();
    this.catalog = null;
    this.superTemplates = superTemplates;
    this.resolved = skipResolve;
  }

  public InheritingJobTemplate(List<JobTemplate> superTemplates) {
    this(superTemplates, true);
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

  /**
   * Resolve all superTemplates being field variables within the class.
   * There are two types of resolution being involved in this method:
   * 1) When all templates are being represented as {@link #superTemplateUris}, the actual template will be loaded from
   * catalog first and enter resolution process. The physical {@link #superTemplates} are being materialized after that.
   * 2) org.apache.gobblin.runtime.template.InheritingJobTemplate#InheritingJobTemplate(java.util.List, boolean) provides
   * interface to directly provide physical {@link #superTemplates}. This case is determined by non-null containers of
   * {@link #superTemplates}.
   *
   */
  private void resolveTemplates(Map<URI, JobTemplate> loadedTemplates) throws SpecNotFoundException, TemplateException {
    if (this.resolved) {
      return;
    }

    if (this.superTemplateUris.size() > 0) {
      this.superTemplates = Lists.newArrayList();
      for (URI uri : this.superTemplateUris) {
        if (!loadedTemplates.containsKey(uri)) {
          JobTemplate newTemplate = this.catalog.getTemplate(uri);
          resolveTemplateRecursionHelper(newTemplate, uri, loadedTemplates);
        }
        this.superTemplates.add(loadedTemplates.get(uri));
      }
    } else if (superTemplates != null ) {
      for (JobTemplate newTemplate : this.superTemplates) {
        if (!loadedTemplates.containsKey(newTemplate.getUri())) {
          resolveTemplateRecursionHelper(newTemplate, newTemplate.getUri(), loadedTemplates);
        }
      }
    }

    this.resolved = true;
  }

  /**
   * The canonicalURI needs to be there when the template needs to loaded from catalog, as the format are adjusted
   * while constructing the template.
   * Essentially, jobCatalog.load(templateUris.get(0)).getUri().equal(templateUris.get(0)) return false.
   */
  private void resolveTemplateRecursionHelper(JobTemplate newTemplate, URI canonicalURI, Map<URI, JobTemplate> loadedTemplates)
      throws SpecNotFoundException, TemplateException {
    loadedTemplates.put(canonicalURI, newTemplate);
    if (newTemplate instanceof InheritingJobTemplate) {
      ((InheritingJobTemplate) newTemplate).resolveTemplates(loadedTemplates);
    }
  }

  public Collection<JobTemplate> getSuperTemplates() throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();

    if (superTemplates != null ) {
      return ImmutableList.copyOf(this.superTemplates);
    } else {
      return ImmutableList.of();
    }
  }

  @Override
  public Config getRawTemplateConfig() throws SpecNotFoundException, TemplateException {
    ensureTemplatesResolved();
    return getRawTemplateConfigHelper(Sets.<JobTemplate>newHashSet());
  }

  private Config getRawTemplateConfigHelper(Set<JobTemplate> alreadyInheritedTemplates)
      throws SpecNotFoundException, TemplateException {
    Config rawTemplate = getLocalRawTemplate();
    if (this.superTemplates != null) {
      for (JobTemplate template : Lists.reverse(this.superTemplates)) {
        if (!alreadyInheritedTemplates.contains(template)) {
          alreadyInheritedTemplates.add(template);
          Config thisFallback = template instanceof InheritingJobTemplate ? ((InheritingJobTemplate) template).getRawTemplateConfigHelper(alreadyInheritedTemplates)
              : template.getRawTemplateConfig();
          rawTemplate = rawTemplate.withFallback(thisFallback);
        }
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
    if (this.superTemplates != null) {
      for (JobTemplate template : this.superTemplates) {
        if (!alreadyLoadedTemplates.contains(template)) {
          alreadyLoadedTemplates.add(template);
          requiredConfigs.addAll(template instanceof InheritingJobTemplate ? ((InheritingJobTemplate) template).getRequiredConfigListHelper(alreadyLoadedTemplates)
              : template.getRequiredConfigList());
        }
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
    if (superTemplates != null ) {
      for (JobTemplate template : Lists.reverse(this.superTemplates)) {
        if (!alreadyLoadedTemplates.contains(template)) {
          alreadyLoadedTemplates.add(template);
          Config fallback = template instanceof InheritingJobTemplate ? ((InheritingJobTemplate) template)
              .getResolvedConfigHelper(config, alreadyLoadedTemplates) : template.getResolvedConfig(config);
          config = config.withFallback(fallback);
        }
      }
    }
    return config;
  }

  protected abstract Config getLocallyResolvedConfig(Config userConfig) throws TemplateException;

}
