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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobCatalogWithTemplates;
import gobblin.runtime.api.SpecNotFoundException;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link gobblin.runtime.api.JobTemplate} that loads a HOCON file as a {@link StaticJobTemplate}.
 */
@Slf4j
public class HOCONInputStreamJobTemplate extends StaticJobTemplate {

  public static final String VERSION_KEY = "gobblin.template.version";
  public static final String DEFAULT_VERSION = "1";

  /**
   * Load a template from an {@link InputStream}. Caller is responsible for closing {@link InputStream}.
   */
  public HOCONInputStreamJobTemplate(InputStream inputStream, URI uri, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException {
    this(ConfigFactory.parseReader(new InputStreamReader(inputStream, Charsets.UTF_8)), uri, catalog);
  }

  private HOCONInputStreamJobTemplate(Config config, URI uri, JobCatalogWithTemplates catalog)
      throws SpecNotFoundException, TemplateException {
    super(uri, config.hasPath(VERSION_KEY) ? config.getString(VERSION_KEY) : DEFAULT_VERSION,
        config.hasPath(ConfigurationKeys.JOB_DESCRIPTION_KEY) ? config.getString(ConfigurationKeys.JOB_DESCRIPTION_KEY) : "",
        config, catalog);
  }
}
