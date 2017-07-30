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
