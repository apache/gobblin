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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import gobblin.util.options.annotations.CheckProperties;
import gobblin.util.options.annotations.Checked;
import gobblin.util.options.example.ExampleClass;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;


/**
 * Finds all user options in a class and any other classes indicated by the root class.
 */
public class OptionFinder {

  public static LoadingCache<Class<?>, Map<String, UserOption>> classCache = CacheBuilder.newBuilder().build(
      new CacheLoader<Class<?>, Map<String, UserOption>>() {
        @Override
        public Map<String, UserOption> load(Class<?> key)
            throws Exception {
          List<UserOption> options = directOptionsForClass(key);
          Map<String, UserOption> optionsMap = Maps.newHashMap();
          for (UserOption option : options) {
            optionsMap.put(option.getKey(), option);
          }
          return optionsMap;
        }
      });

  public static void main(String[] args) throws Exception {

    if (args.length != 0) {
      System.out.println(String.format("Usage: java -cp <classpath> %s <class>",
          OptionFinder.class.getCanonicalName()));
      System.exit(1);
    }

    // Class<?> classToCheck = Class.forName(args[0]);
    Class<?> classToCheck = ExampleClass.class;

    System.out.println(new OptionFinder().getJsonOptionsForClass(classToCheck, new Properties()));
  }

  /**
   * Get user options for input class as a Json string.
   * @throws IOException
   */
  public String getJsonOptionsForClass(Class<?> klazz, Properties properties) throws IOException {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new ClassAdapterFactory());
    Options options = findOptionsForClass(klazz, properties);
    Map<Class<?>, CheckedClass> filteredMap = Maps.filterValues(options, new Predicate<CheckedClass>() {
      @Override
      public boolean apply(@Nullable CheckedClass input) {
        return !input.getUserOptions().isEmpty();
      }
    });
    return builder.create().toJson(filteredMap);
  }

  public static class ClassAdapterFactory implements TypeAdapterFactory {
    @Override public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      if (type.getRawType() == Class.class) {
        return (TypeAdapter<T>) new ClassAdapter();
      } else {
        return null;
      }
    }

    public static class ClassAdapter extends TypeAdapter<Class<?>> {
      @Override public void write(JsonWriter out, Class<?> value) throws IOException {
        if (value == null) {
          out.nullValue();
        } else {
          out.value(value.getName());
        }
      }

      @Override public Class<?> read(JsonReader in) throws IOException {
        try {
          return Class.forName(in.nextString());
        } catch (ReflectiveOperationException roe) {
          throw new IOException(roe);
        }
      }
    }
  }

  @Data
  @Builder
  public static class CheckedClass {
    private final Class<?> theClass;
    private final String shortName;
    private final boolean checked;
    private final List<UserOption> userOptions = Lists.newArrayList();

    public static CheckedClass fromChecked(Class<?> fromClass) {
      Checked checked = fromClass.getAnnotation(Checked.class);
      CheckedClassBuilder builder = CheckedClass.builder().theClass(fromClass).checked(true);
      if (!Strings.isNullOrEmpty(checked.shortName())) {
        builder.shortName(checked.shortName());
      }
      return builder.build();
    }
  }

  public static class Options extends HashMap<Class<?>, CheckedClass> {
  };

  public Options findOptionsForClass(Class<?> klazz, Properties properties) throws IOException {
    Options options = new Options();
    findOptionsForClass(options, klazz, properties);
    return options;
  }

  private void findOptionsForClass(Options options, Class<?> klazz, Properties properties) throws IOException {
    if (options.containsKey(klazz)) {
      return;
    }

    findDeclaredOptionsForClass(options, klazz, Optional.<Class<?>>absent());
    Set<Class<?>> instantiatedClasses = getNewInstantiatedClasses(options, properties);
    for (Class<?> instantiatedClass : instantiatedClasses) {
      findOptionsForClass(options, instantiatedClass, properties);
    }
  }

  private Set<Class<?>> getNewInstantiatedClasses(Options options, Properties properties) {
    Set<Class<?>> classes = Sets.newHashSet();
    for (CheckedClass checkedClass : options.values()) {
      for (UserOption option : checkedClass.getUserOptions()) {
        if (option.getInstantiatesClass() != null && properties.containsKey(option.getKey())) {
          try {
            Class<?> instantiatedClass = Class.forName(properties.getProperty(option.getKey()));
            if (!options.containsKey(instantiatedClass)) {
              classes.add(instantiatedClass);
            }
          } catch (ReflectiveOperationException roe) {
            // do nothing
          }
        }
        if (!option.getClassInstantiationMap().isEmpty()) {
          String optionValue = properties.getProperty(option.getKey(), "");
          Class<?> instantiated = option.getClassInstantiationMap().get(optionValue);
          if (instantiated != null && !options.containsKey(instantiated)) {
            classes.add(instantiated);
          }
        }
      }
    }
    return classes;
  }

  /**
   * Get user options of input class as a map from class to {@link UserOption}.
   * @throws IOException
   */
  private void findDeclaredOptionsForClass(Options options, Class<?> klazz, Optional<Class<?>> alias) throws IOException {

    Class<?> actualAlias = alias.or(klazz);

    if (!options.containsKey(actualAlias)) {
      CheckedClass checkedClass = klazz.isAnnotationPresent(Checked.class)
          ? CheckedClass.fromChecked(klazz)
          : CheckedClass.builder().theClass(klazz).checked(false).build();

      options.put(klazz, checkedClass);
    }

    CheckedClass checkedClass = options.get(actualAlias);
    checkedClass.getUserOptions().addAll(directOptionsForClass(klazz));

    if (klazz.getSuperclass() != Object.class) {
      findDeclaredOptionsForClass(options, klazz.getSuperclass(), Optional.<Class<?>>of(actualAlias));
    }

    for (Field field : klazz.getDeclaredFields()) {
      if (field.isAnnotationPresent(CheckProperties.class)) {
        findDeclaredOptionsForClass(options, field.getType(), Optional.<Class<?>>absent());
      }
    }

    if (klazz.isAnnotationPresent(Checked.class)) {
      Checked annotation = klazz.getAnnotation(Checked.class);
      for (Class<?> checkClass : annotation.checkClasses()) {
        findDeclaredOptionsForClass(options, checkClass, Optional.<Class<?>>absent());
      }
      Map<String, UserOption> relevantOptions = Maps.newHashMap();
      for (Class<?> configurationClass : annotation.configurationClasses()) {
        try {
          relevantOptions.putAll(classCache.get(configurationClass));
        } catch (ExecutionException ee) {
          throw new IOException(ee);
        }
      }
      for (String optionName : annotation.userOptions()) {
        if (relevantOptions.containsKey(optionName)) {
          checkedClass.getUserOptions().add(relevantOptions.get(optionName));
        } else {
          throw new IOException("Configuration not found: " + optionName);
        }
      }
    }
  }

  private static List<UserOption> directOptionsForClass(Class<?> klazz) throws IOException {
    List<UserOption> options = Lists.newArrayList();
    for (Field field : klazz.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers())
          && String.class.isAssignableFrom(field.getType()) && field.isAnnotationPresent(
          gobblin.util.options.annotations.UserOption.class)) {
        options.add(processAnnotatedField(field));
      }
    }
    return options;
  }

  private static UserOption processAnnotatedField(Field field) throws IOException {

    try {
      UserOption.UserOptionBuilder builder = UserOption.builder().key((String) field.get(null));
      gobblin.util.options.annotations.UserOption annotation =
          field.getAnnotation(gobblin.util.options.annotations.UserOption.class);
      builder.description(annotation.description());
      builder.declaringClass(field.getDeclaringClass());
      if (annotation.required()) {
        builder.required(true);
      }
      if (annotation.instantiates() != null) {
        builder.instantiatesClass(annotation.instantiates());
      }
      if (annotation.values() != null) {
        builder.values(annotation.values());
      }
      if (!Strings.isNullOrEmpty(annotation.shortName())) {
        builder.shortName(annotation.shortName());
      }
      builder.type(annotation.type());
      builder.advanced(annotation.advanced());

      return builder.build();

    } catch (IllegalAccessException iae) {
      // do nothing, this shouldn't happen
      throw new RuntimeException("This shouldn't happen.");
    }
  }

}
