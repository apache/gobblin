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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link gobblin.runtime.api.JobTemplate} that reads the template information from a resources file.
 *
 * This class is final because otherwise we could not guarantee the {@link InputStream} to be closed if the super
 * constructor throws an exception.
 */
@Slf4j
public final class ResourceBasedJobTemplate extends HOCONInputStreamJobTemplate {

  public static final String SCHEME = "resource";

  public static ResourceBasedJobTemplate forURI(URI uri, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException, IOException {
    try (InputStream is = getInputStreamForURI(uri)) {
      return new ResourceBasedJobTemplate(is, uri, catalog);
    }
  }

  public static ResourceBasedJobTemplate forResourcePath(String path)
      throws SpecNotFoundException, TemplateException, IOException, URISyntaxException {
    return forResourcePath(path, new PackagedTemplatesJobCatalogDecorator());
  }

  public static ResourceBasedJobTemplate forResourcePath(String path, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException, IOException, URISyntaxException {
    return forURI(new URI(path), catalog);
  }


  /**
   * Initializes the template by retrieving the specified template file and obtain some special attributes.
   */
  private ResourceBasedJobTemplate(InputStream is, URI uri, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException, IOException {
    super(is, uri, catalog);
  }

  private static InputStream getInputStreamForURI(URI uri) throws IOException {
    Preconditions.checkArgument(uri.getScheme() == null || uri.getScheme().equals(SCHEME), "Unexpected template scheme: " + uri);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(uri.getPath()), "Template path is null: " + uri);

    log.info("Loading the resource based job configuration template " + uri);

    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    InputStream is = ResourceBasedJobTemplate.class.getClassLoader().getResourceAsStream(path);
    if (is == null) {
      throw new IOException(String.format("Could not find resource at path %s required to load template %s.", path, uri));
    }

    return is;
  }

}
