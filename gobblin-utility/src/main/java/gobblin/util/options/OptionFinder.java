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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import gobblin.util.options.annotations.CheckProperties;
import gobblin.util.options.annotations.Checked;


/**
 * Created by ibuenros on 1/23/16.
 */
public class OptionFinder {

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      System.out.println(String.format("Usage: java -cp <classpath> %s <class>",
          OptionFinder.class.getCanonicalName()));
      System.exit(1);
    }

    Class<?> classToCheck = Class.forName(args[0]);

    System.out.println(new OptionFinder().getJsonOptionsForClass(classToCheck));
  }

  public String getJsonOptionsForClass(Class<?> klazz) throws IOException {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Class.class, new TypeAdapter<Class<?>>() {
      @Override public void write(JsonWriter out, Class value) throws IOException {
        out.value(value.getCanonicalName());
      }

      @Override public Class read(JsonReader in) throws IOException {
        String className = in.nextString();
        try {
          return Class.forName(className);
        } catch (ReflectiveOperationException roe) {
          throw new IOException(roe);
        }
      }
    });
    return builder.create().toJson(findOptionsForClass(klazz));
  }

  public Map<Class<?>, List<UserOption>> findOptionsForClass(Class<?> klazz) throws IOException {

    Map<Class<?>, List<UserOption>> options = Maps.newConcurrentMap();
    options.put(klazz, Lists.<UserOption>newArrayList());

    for (Field field : klazz.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers())
          && String.class.isAssignableFrom(field.getType())) {
        options.get(klazz).add(processAnnotatedField(field));
      }
      if (field.isAnnotationPresent(CheckProperties.class)) {
        options.putAll(findOptionsForClass(field.getType()));
      }
    }

    if (klazz.isAnnotationPresent(Checked.class)) {
      Checked annotation = klazz.getAnnotation(Checked.class);
      for (Class<?> checkClass : annotation.checkClasses()) {
        options.putAll(findOptionsForClass(checkClass));
      }
    }

    return options;
  }

  private UserOption processAnnotatedField(Field field) {

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

      return builder.build();

    }catch (IllegalAccessException iae) {
      // do nothing, this shouldn't happen
      throw new RuntimeException("This shouldn't happen.");
    }
  }

}
