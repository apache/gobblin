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
package gobblin.util.callbacks;

import com.google.common.base.Function;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A helper class to wrap a function and provide a name for logging purposes
 */
@Getter
@AllArgsConstructor
public abstract class Callback<L, R> implements Function<L, R> {
  private final String name;

  @Override public String toString() {
    return this.name;
  }
}
