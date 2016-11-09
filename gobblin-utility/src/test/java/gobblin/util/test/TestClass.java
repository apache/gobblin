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

package gobblin.util.test;

import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Used for {@link gobblin.util.io.GsonInterfaceAdapterTest}.
 */
@EqualsAndHashCode(callSuper = true)
public class TestClass extends BaseClass {

  private static final Random random = new Random();

  private final int intValue = random.nextInt();
  private final long longValue = random.nextLong();
  private final double doubleValue = random.nextLong();
  private final Map<String, Integer> map = createRandomMap();
  private final List<String> list = createRandomList();
  private final Optional<String> present = Optional.of(Integer.toString(random.nextInt()));
  // Set manually to absent
  public Optional<String> absent = Optional.of("a");
  private final Optional<BaseClass> optionalObject = Optional.of(new BaseClass());
  private final BaseClass polymorphic = new ExtendedClass();
  private final Optional<? extends BaseClass> polymorphicOptional = Optional.of(new ExtendedClass());

  private static Map<String, Integer> createRandomMap() {
    Map<String, Integer> map = Maps.newHashMap();
    int size = random.nextInt(5);
    for (int i = 0; i < size; i++) {
      map.put("value" + random.nextInt(), random.nextInt());
    }
    return map;
  }

  private static List<String> createRandomList() {
    List<String> list = Lists.newArrayList();
    int size = random.nextInt(5);
    for (int i = 0; i < size; i++) {
      list.add("value" + random.nextInt());
    }
    return list;
  }

}
