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

package gobblin.runtime.job_catalog;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.template.ResourceBasedJobTemplate;
import gobblin.util.Decorator;

import lombok.experimental.Delegate;


/**
 * A {@link Decorator} for {@link JobCatalog} that loads {@link JobTemplate}s from classpath (resources and classes).
 *
 * This decorator will intercept {@link JobTemplate} requests with schemes "resource" or "class", and will load them
 * from classpath.
 * * A class template has uri "class://fully.qualified.class.name", and will instantiate that class and attempt to cast
 *    as {@link JobTemplate}.
 * * A resource template has uri "resource:///path/to/template/in/resources.template", and the decorator will attempt to
 *    parse it as a {@link ResourceBasedJobTemplate}.
 * * Any other scheme will be delegated to the underlying job catalog if it implements {@link JobCatalogWithTemplates},
 *    or a {@link SpecNotFoundException} will be thrown.
 */
public class PackagedTemplatesJobCatalogDecorator implements Decorator, JobCatalogWithTemplates {

  public static final String RESOURCE = "resource";
  public static final String CLASS = "class";

  /**
   * Creates a {@link PackagedTemplatesJobCatalogDecorator} with an underlying empty {@link InMemoryJobCatalog}.
   */
  public PackagedTemplatesJobCatalogDecorator() {
    this(new InMemoryJobCatalog());
  }

  @Delegate
  private final JobCatalog underlying;

  public PackagedTemplatesJobCatalogDecorator(JobCatalog underlying) {
    this.underlying = underlying != null ? underlying : new InMemoryJobCatalog();
  }

  @Override
  public Object getDecoratedObject() {
    return this.underlying;
  }

  @Override
  public JobTemplate getTemplate(URI uri)
      throws SpecNotFoundException, JobTemplate.TemplateException {
    if (RESOURCE.equals(uri.getScheme())) {
      try {
        // resources are accessed by relative uris, make the uri relative (get path, strip initial /)
        URI actualResourceUri = new URI(uri.getPath().substring(1));
        return ResourceBasedJobTemplate.forURI(actualResourceUri, this);
      } catch (URISyntaxException use) {
        throw new RuntimeException("Error when computing resource path.", use);
      } catch (IOException ioe) {
        throw new SpecNotFoundException(uri, ioe);
      }
    } else if(CLASS.equals(uri.getScheme())) {
      try {
        return ((Class<? extends JobTemplate>) Class.forName(uri.getAuthority())).newInstance();
      } catch (ReflectiveOperationException roe) {
        throw new SpecNotFoundException(uri, roe);
      }
    }
    if (this.underlying != null && this.underlying instanceof JobCatalogWithTemplates) {
      JobTemplate template = ((JobCatalogWithTemplates) this.underlying).getTemplate(uri);
      if (template == null) {
        throw new SpecNotFoundException(uri);
      }
      return template;
    }
    throw new SpecNotFoundException(uri);
  }

  @Override
  public Collection<JobTemplate> getAllTemplates() {
    throw new UnsupportedOperationException();
  }

}
