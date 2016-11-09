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

package gobblin.util.request_allocation;

import com.typesafe.config.Config;

import gobblin.util.ClassAliasResolver;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RequestAllocatorUtils {

  public static final String ALLOCATOR_ALIAS_KEY = "requestAllocatorAlias";

  /**
   * Infer and construct a {@link RequestAllocator} from an input {@link Config}.
   */
  public static <T extends Request<T>> RequestAllocator<T> inferFromConfig(RequestAllocatorConfig<T> configuration) {
    try {
      String alias = configuration.getLimitedScopeConfig().hasPath(ALLOCATOR_ALIAS_KEY) ?
          configuration.getLimitedScopeConfig().getString(ALLOCATOR_ALIAS_KEY) :
          BruteForceAllocator.Factory.class.getName();
      RequestAllocator.Factory allocatorFactory = new ClassAliasResolver<>(RequestAllocator.Factory.class).
          resolveClass(alias).newInstance();

      log.info("Using allocator factory " + allocatorFactory.getClass().getName());

      return allocatorFactory.createRequestAllocator(configuration);
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException(roe);
    }
  }
}
