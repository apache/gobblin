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
 *
 * <br>
 * <b>
 *   Note: For the moment this class will only look for classes with gobblin prefix, as scanning the entire classpath is
 *   very expensive.
 * </b>
 */
@Slf4j
public class ClassAliasResolver<T> {

  Map<String, Class<? extends T>> aliasToClassCache;
  private final Class<T> subtypeOf;

  public ClassAliasResolver(Class<T> subTypeOf) {
    Map<String, Class<? extends T>> cache = Maps.newHashMap();
    // Scan all packages
    Reflections reflections = new Reflections("gobblin");
    for (Class<? extends T> clazz : reflections.getSubTypesOf(subTypeOf)) {
      if (clazz.isAnnotationPresent(Alias.class)) {
        String alias = clazz.getAnnotation(Alias.class).value();
        if (cache.containsKey(alias)) {
          log.warn(String.format("Alias %s already mapped to class %s. Mapping for %s will be ignored", alias,
              cache.get(alias).getCanonicalName(), clazz.getCanonicalName()));
        } else {
          cache.put(clazz.getAnnotation(Alias.class).value(), clazz);
        }
      }
    }
    this.subtypeOf = subTypeOf;
    this.aliasToClassCache = ImmutableMap.copyOf(cache);
  }

  /**
   * Resolves the given alias to its name if a mapping exits. To create a mapping for a class,
   * it has to be annotated with {@link Alias}
   *
   * @param possibleAlias to use for resolution.
   * @return The name of the class with <code>possibleAlias</code> if mapping is available.
   * Return the input <code>possibleAlias</code> if no mapping is found.
   */
  public String resolve(final String possibleAlias) {
    if (this.aliasToClassCache.containsKey(possibleAlias)) {
      return this.aliasToClassCache.get(possibleAlias).getName();
    }
    return possibleAlias;
  }

  /**
   * Attempts to resolve the given alias to a class. It first tries to find a class in the classpath with that alias
   * and which is a subclass of {@link #subtypeOf}, if it fails it tries to find a class in the classpath with the
   * exact input name.
   */
  public Class<? extends T> resolveClass(final String aliasOrClassName) throws ClassNotFoundException {
    if (this.aliasToClassCache.containsKey(aliasOrClassName)) {
      return this.aliasToClassCache.get(aliasOrClassName);
    }
    try {
      return Class.forName(aliasOrClassName).asSubclass(this.subtypeOf);
    } catch (ClassCastException cce) {
      throw new ClassNotFoundException(
          String.format("Found class %s but it cannot be cast to %s.", aliasOrClassName, this.subtypeOf.getName()), cce);
    }
  }
}
