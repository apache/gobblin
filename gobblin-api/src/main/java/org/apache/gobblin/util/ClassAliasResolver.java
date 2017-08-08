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
package org.apache.gobblin.util;

import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.annotation.Alias;


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
 *   Note: For the moment this class will only look for classes with gobblin/com.linkedin.gobblin prefix, as scanning
 *   the entire classpath is very expensive.
 * </b>
 */
@Slf4j
public class ClassAliasResolver<T> {

  // Scan all packages in the classpath with prefix gobblin, com.linkedin.gobblin when class is loaded.
  // Since scan is expensive we do it only once when class is loaded.
  private static final Reflections REFLECTIONS = new Reflections(new ConfigurationBuilder().forPackages("gobblin",
      "com.linkedin.gobblin", "org.apache.gobblin"));

  Map<String, Class<? extends T>> aliasToClassCache;
  private final List<Alias> aliasObjects;
  private final Class<T> subtypeOf;

  public ClassAliasResolver(Class<T> subTypeOf) {
    Map<String, Class<? extends T>> cache = Maps.newHashMap();
    this.aliasObjects = Lists.newArrayList();
    for (Class<? extends T> clazz : REFLECTIONS.getSubTypesOf(subTypeOf)) {
      if (clazz.isAnnotationPresent(Alias.class)) {
        Alias aliasObject = clazz.getAnnotation(Alias.class);
        String alias = aliasObject.value().toUpperCase();
        if (cache.containsKey(alias)) {
          log.warn(String.format("Alias %s already mapped to class %s. Mapping for %s will be ignored", alias,
              cache.get(alias).getCanonicalName(), clazz.getCanonicalName()));
        } else {
          aliasObjects.add(aliasObject);
          cache.put(clazz.getAnnotation(Alias.class).value().toUpperCase(), clazz);
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
    if (this.aliasToClassCache.containsKey(possibleAlias.toUpperCase())) {
      return this.aliasToClassCache.get(possibleAlias.toUpperCase()).getName();
    }
    return possibleAlias;
  }

  /**
   * Attempts to resolve the given alias to a class. It first tries to find a class in the classpath with this alias
   * and is also a subclass of {@link #subtypeOf}, if it fails it returns a class object for name
   * <code>aliasOrClassName</code>.
   */
  public Class<? extends T> resolveClass(final String aliasOrClassName) throws ClassNotFoundException {
    if (this.aliasToClassCache.containsKey(aliasOrClassName.toUpperCase())) {
      return this.aliasToClassCache.get(aliasOrClassName.toUpperCase());
    }
    try {
      return Class.forName(aliasOrClassName).asSubclass(this.subtypeOf);
    } catch (ClassCastException cce) {
      throw new ClassNotFoundException(
          String.format("Found class %s but it cannot be cast to %s.", aliasOrClassName, this.subtypeOf.getName()), cce);
    }
  }

  /**
   * Get the map from found aliases to classes.
   */
  public Map<String, Class<? extends T>> getAliasMap() {
    return ImmutableMap.copyOf(this.aliasToClassCache);
  }

  public List<Alias> getAliasObjects() {
    return ImmutableList.copyOf(this.aliasObjects);
  }
}
