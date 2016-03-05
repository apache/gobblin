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

package gobblin.util.options;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import gobblin.util.options.annotations.ClassInstantiationMap;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

import java.util.Set;

import com.google.common.collect.Sets;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;


/**
 * A user option.
 */
@Getter
public class UserOption {

  private static final Reflections reflections = new Reflections(new ConfigurationBuilder().
      setUrls(ClasspathHelper.forPackage("gobblin")).setScanners(new SubTypesScanner()));

  /** The key for this option. */
  private final String key;
  /** Description for the option. */
  private final String description;
  /** Whether option is required. */
  private final boolean required;
  /**
   * Indicates that an object with the class indicated by the value of this option will be instantiated, and that the
   * instantiated class must be a subclass of this class.
   */
  private final Class<?> instantiatesClass;
  /** Indicates that the value of the option must be one of the values of this enum. */
  private final Class<? extends Enum<?>> values;
  /** Stringification of {@link #values} */
  private final Set<String> valueStrings;
  /** The class that declares this option. */
  private final Class<?> declaringClass;
  /** A map from values to the class that gets instantiated. */
  private final Map<String, Class<?>> classInstantiationMap;
  /** Short name for the property. */
  private final String shortName;
  /** Should options be recomputed on change of this value. */
  private final boolean requiresRecompute;
  /** Type of the user option. */
  private final gobblin.util.options.annotations.UserOption.OptionType type;
  /** Advanced options */
  private final boolean advanced;

  @Builder
  public UserOption(String key, String description, boolean required, Class<?> instantiatesClass,
      Class<? extends Enum<?>> values, Class<?> declaringClass, String shortName,
      gobblin.util.options.annotations.UserOption.OptionType type, boolean advanced) throws IOException {
    this.key = key;
    this.description = description;
    this.required = required;
    this.instantiatesClass = instantiatesClass == Void.class ? null : instantiatesClass;
    this.values = values == DummyEnum.class ? null : values;
    this.valueStrings = Sets.newHashSet();
    this.classInstantiationMap = Maps.newHashMap();
    this.type = type;
    if (this.values != null) {
      for (Enum<?> e : this.values.getEnumConstants()) {
        this.valueStrings.add(e.name());
      }
      for (Field field : this.values.getFields()) {
        if (field.isAnnotationPresent(ClassInstantiationMap.class)) {
          try {
            Map<Enum<?>, Class<?>> classInstantiationMapTmp = (Map<Enum<?>, Class<?>>) field.get(null);
            for (Map.Entry<Enum<?>, Class<?>> entry : classInstantiationMapTmp.entrySet()) {
              this.classInstantiationMap.put(entry.getKey().name(), entry.getValue());
            }
          } catch (ReflectiveOperationException roe) {
            throw new IOException(roe);
          }
        }
      }
    } else if (this.type == gobblin.util.options.annotations.UserOption.OptionType.BOOLEAN) {
      this.valueStrings.addAll(Lists.newArrayList(Boolean.toString(true), Boolean.toString(false)));
    } else if (this.instantiatesClass != null) {
      for (Class klazz : reflections.getSubTypesOf(this.instantiatesClass)) {
        if (!(Modifier.isAbstract(klazz.getModifiers()) || Modifier.isInterface(klazz.getModifiers()))) {
          this.valueStrings.add(klazz.getName());
          this.classInstantiationMap.put(klazz.getName(), klazz);
        }
      }
    }
    this.declaringClass = declaringClass;
    this.shortName = shortName;
    this.requiresRecompute = instantiatesClass != Void.class || !this.classInstantiationMap.isEmpty();
    this.advanced = advanced;
  }
}
