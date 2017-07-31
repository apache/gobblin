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

package org.apache.gobblin.runtime.api;

import com.typesafe.config.Config;

import java.util.Collection;


/**
 * An interface for claiming methods used for
 * retrieving template configs
 * and properties that are required by user to fit in.
 * <p>
 *   Each data source may have its own Job Template.
 *   Examples are available based on requests.
 * </p>
 *
 */
public interface JobTemplate extends Spec {

  /**
   * Retrieve all configuration inside pre-written template.
   * @return
   */
  Config getRawTemplateConfig() throws SpecNotFoundException, TemplateException;

  /**
   * Retrieve all configs that are required from user to fill.
   * @return
   */
  Collection<String> getRequiredConfigList() throws SpecNotFoundException, TemplateException;

  /**
   * Return the combine configuration of template and user customized attributes.
   * @return
   */
  Config getResolvedConfig(Config userProps) throws SpecNotFoundException, TemplateException;

  class TemplateException extends Exception {
    public TemplateException(String message, Throwable cause) {
      super(message, cause);
    }

    public TemplateException(String message) {
      super(message);
    }
  }
}
