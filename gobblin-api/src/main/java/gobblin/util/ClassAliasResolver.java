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
package gobblin.util;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.reflections.Reflections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.annotation.Alias;


/**
 * A class that scans through all classes in the classpath annotated with {@link Alias} and is a subType of <code>subTypeOf</code> constructor argument.
 * It caches the alias to class mapping. Applications can call {@link #resolve(String)} to resolve a class's alias to its
 * cannonical name.
 *
 * <br>
 * <b>Note : If same alias is used for multiple classes in the classpath, the first mapping in classpath scan holds good.
 * Subsequent ones will be logged and ignored.
 * </b>
 */
@Slf4j
public class ClassAliasResolver {

  Map<String, Class<?>> aliasToClassCache;

  public ClassAliasResolver(Class<?> subTypeOf) {
    Map<String, Class<?>> cache = Maps.newHashMap();
    // Scan all packages
    Reflections reflections = new Reflections("");
    for (Class<?> clazz : Sets.intersection(reflections.getTypesAnnotatedWith(Alias.class), reflections.getSubTypesOf(subTypeOf))) {
      String alias = clazz.getAnnotation(Alias.class).value();
      if (cache.containsKey(alias)) {
        log.warn(String.format("Alias %s already mapped to class %s. Mapping for %s will be ignored", alias,
            cache.get(alias).getCanonicalName(), clazz.getCanonicalName()));
      } else {
        cache.put(clazz.getAnnotation(Alias.class).value(), clazz);
      }

    }
    this.aliasToClassCache = ImmutableMap.copyOf(cache);
  }

  /**
   * Resolves the given alias to its cannonical name if a mapping exits. To create a mapping for a class,
   * it has to be annotated with {@link Alias}
   *
   * @param possibleAlias to use for resolution.
   * @return The cannonical name of the class with <code>possibleAlias</code> if mapping is available.
   * Return the input <code>possibleAlias</code> if no mapping is found.
   */
  public String resolve(final String possibleAlias) {
    if (aliasToClassCache.containsKey(possibleAlias)) {
      return aliasToClassCache.get(possibleAlias).getCanonicalName();
    }
    return possibleAlias;
  }
}
