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

package gobblin.runtime.job_spec;

import java.net.URI;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.job_catalog.InMemoryJobCatalog;
import gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import gobblin.util.ConfigUtils;

import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * A job spec whose template has been applied to its configuration.
 */
public class ResolvedJobSpec extends JobSpec {

  @Getter
  private final JobSpec originalJobSpec;

  public ResolvedJobSpec(JobSpec other) throws SpecNotFoundException, JobTemplate.TemplateException {
    this(other, new InMemoryJobCatalog());
  }

  public ResolvedJobSpec(JobSpec other, GobblinInstanceDriver driver)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    this(other, driver.getJobCatalog());
  }

  /**
   * Resolve the job spec using classpath templates as well as any templates available in the input {@link JobCatalog}.
   */
  public ResolvedJobSpec(JobSpec other, JobCatalog catalog)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    super(other.getUri(), other.getVersion(), other.getDescription(), resolveConfig(other, catalog),
        ConfigUtils.configToProperties(resolveConfig(other, catalog)), other.getTemplateURI());
    this.originalJobSpec = other;
  }

  private static Config resolveConfig(JobSpec jobSpec, JobCatalog catalog)
      throws SpecNotFoundException, JobTemplate.TemplateException {

    Optional<URI> templateURIOpt = jobSpec.getTemplateURI();
    if (templateURIOpt.isPresent()) {
      JobCatalogWithTemplates catalogWithTemplates = new PackagedTemplatesJobCatalogDecorator(catalog);
      JobTemplate template = catalogWithTemplates.getTemplate(templateURIOpt.get());
      return template.getResolvedConfig(jobSpec.getConfig()).resolve();
    } else {
      return jobSpec.getConfig().resolve();
    }

  }

  @Override
  public boolean equals(Object other) {
    return other instanceof  ResolvedJobSpec && super.equals(other) &&
        this.originalJobSpec.equals(((ResolvedJobSpec) other).originalJobSpec);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + originalJobSpec.hashCode();
    return result;
  }
}
