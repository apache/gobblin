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

package gobblin.util.options.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import gobblin.util.options.DummyEnum;


/**
 * Used to annotate fields of type public static String, indicating that the class expects the input
 * {@link java.util.Properties} object to contain a key-value pair with that key.
 *
 * See {@link gobblin.util.options.UserOption} for descriptions of the fields of this annotation.
 */
@Retention(RetentionPolicy.RUNTIME) @Target(value= ElementType.FIELD)
public @interface UserOption {

  boolean required() default false;
  Class<?> instantiates() default Void.class;
  Class<? extends Enum<?>> values() default DummyEnum.class;
  String description() default "";
  String shortName() default "";
  OptionType type() default OptionType.STRING;
  boolean advanced() default false;

  public static enum OptionType {
    STRING, INT, BOOLEAN
  }

}
