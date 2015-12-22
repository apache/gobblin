/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.context.filter;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.reflect.ConstructorUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;


/**
 * Factory for {@link ContextFilter}s.
 */
@Slf4j
public class ContextFilterFactory {

  public static final String CONTEXT_FILTER_CLASS = "context.filter.class";

  /**
   * Modify the configuration to set the {@link ContextFilter} class.
   * @param config Input {@link Config}.
   * @param klazz Class of desired {@link ContextFilter}.
   * @return Modified {@link Config}.
   */
  public static Config setContextFilterClass(Config config, Class<? extends ContextFilter> klazz) {
    return config.withValue(CONTEXT_FILTER_CLASS, ConfigValueFactory.fromAnyRef(klazz.getCanonicalName()));
  }

  /**
   * Create a {@link ContextFilter} from a {@link Config}.
   * @param config {@link Config} used for creating new {@link ContextFilter}.
   * @return a new {@link ContextFilter}.
   */
  public static ContextFilter createContextFilter(Config config) {
    // For now always return an accept-all context filter.
    if (config.hasPath(CONTEXT_FILTER_CLASS)) {
      try {
        return ContextFilter.class.cast(
            ConstructorUtils.invokeConstructor(Class.forName(config.getString(CONTEXT_FILTER_CLASS)), config));
      } catch (ReflectiveOperationException rfe) {
        log.error("Failed to instantiate context filter with class " + config.getString(CONTEXT_FILTER_CLASS), rfe);
      }
    }
    return new AllContextFilter();
  }
}
