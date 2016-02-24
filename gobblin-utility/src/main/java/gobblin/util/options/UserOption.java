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

import lombok.Builder;
import lombok.Getter;

import java.util.Set;

import com.google.common.collect.Sets;


/**
 * A user option.
 */
@Getter
public class UserOption {

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

  @Builder
  public UserOption(String key, String description, boolean required, Class<?> instantiatesClass,
      Class<? extends Enum<?>> values, Class<?> declaringClass) {
    this.key = key;
    this.description = description;
    this.required = required;
    this.instantiatesClass = instantiatesClass == Void.class ? null : instantiatesClass;
    this.values = values == DummyEnum.class ? null : values;
    this.valueStrings = Sets.newHashSet();
    if (this.values != null) {
      for (Enum<?> e : this.values.getEnumConstants()) {
        this.valueStrings.add(e.name());
      }
    }
    this.declaringClass = declaringClass;
  }
}
