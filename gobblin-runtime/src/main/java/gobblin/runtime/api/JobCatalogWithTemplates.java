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

package gobblin.runtime.api;

import java.net.URI;
import java.util.Collection;


/**
 * A {@link JobTemplate} that supports loading {@link JobTemplate}s.
 */
public interface JobCatalogWithTemplates extends JobCatalog {
  /**
   * Get {@link JobTemplate} with given {@link URI}.
   * @throws SpecNotFoundException if a {@link JobTemplate} with given {@link URI} cannot be found.
   */
  JobTemplate getTemplate(URI uri) throws SpecNotFoundException, JobTemplate.TemplateException;

  /**
   * List all {@link JobTemplate}s available.
   */
  Collection<JobTemplate> getAllTemplates();
}
