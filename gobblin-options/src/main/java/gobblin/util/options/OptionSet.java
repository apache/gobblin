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

import lombok.Getter;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * A set of user options.
 * TODO: use it in {@link OptionFinder}.
 */
@Getter
public class OptionSet {

  private final List<UserOption> options = Lists.newArrayList();
  private final Set<Class<?>> classesTraversed = Sets.newHashSet();
  private final Set<Class<?>> uncheckedClasses = Sets.newHashSet();

  public void add(OptionSet other) {
    this.options.addAll(other.options);
    this.classesTraversed.addAll(other.classesTraversed);
    this.uncheckedClasses.addAll(other.uncheckedClasses);
  }

}
