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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import com.google.common.collect.ImmutableList;

import gobblin.util.options.annotations.CheckProperties;
import gobblin.util.options.annotations.Checked;


/**
 * Created by ibuenros on 1/23/16.
 */
public class OptionFinder {

  public OptionSet findOptionsForClass(Class<?> klazz) throws IOException {

    OptionSet optionSet = new OptionSet();
    optionSet.getClassesTraversed().add(klazz);

    for (Field field : klazz.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers())
          && String.class.isAssignableFrom(field.getType())) {
        optionSet.getOptions().add(processAnnotatedField(field));
      }
      if (field.isAnnotationPresent(CheckProperties.class)) {
        optionSet.add(findOptionsForClass(field.getType()));
      }
    }

    if (klazz.isAnnotationPresent(Checked.class)) {
      Checked annotation = klazz.getAnnotation(Checked.class);
      for (Class<?> checkClass : annotation.checkClasses()) {
        optionSet.add(findOptionsForClass(checkClass));
      }
    } else {
      optionSet.getUncheckedClasses().add(klazz);
    }

    return optionSet;
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
