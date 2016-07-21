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
package gobblin.configuration;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
/**
 * Annotation that marks a field as a Gobblin configuration key
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigKey {
  /**
   * If this config key is required
   */
  boolean required() default false;
  /**
   * Default value for this config key
   */
  String def() default "None";
  /**
   * Group of this config key. All keys of the same group will be documented together
   */
  String group() default "";
}
