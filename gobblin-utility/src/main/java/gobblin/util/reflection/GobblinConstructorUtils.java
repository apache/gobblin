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
package gobblin.util.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;


/**
 * Helper methods to instantiate classes
 */
@Slf4j
public class GobblinConstructorUtils {

  /**
   * Convenience method on top of {@link ConstructorUtils#invokeConstructor(Class, Object[])} that returns a new
   * instance of the <code>cls</code> based on a constructor priority order. Each {@link List} in the
   * <code>constructorArgs</code> array contains the arguments for a constructor of <code>cls</code>. The first
   * constructor whose signature matches the argument types will be invoked.
   *
   * @param cls the class to be instantiated
   * @param constructorArgs An array of constructor argument list. Order defines the priority of a constructor.
   * @return
   *
   * @throws NoSuchMethodException if no constructor matched was found
   */
  @SafeVarargs
  public static <T> T invokeFirstConstructor(Class<T> cls, List<Object>... constructorArgs)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    for (List<Object> args : constructorArgs) {

      Class<?>[] parameterTypes = new Class[args.size()];
      for (int i = 0; i < args.size(); i++) {
        parameterTypes[i] = args.get(i).getClass();
      }

      if (ConstructorUtils.getMatchingAccessibleConstructor(cls, parameterTypes) != null) {
        log.info(
            String.format("Found accessible constructor with parameter types %s found", Arrays.asList(parameterTypes)));
        return ConstructorUtils.invokeConstructor(cls, args.toArray(new Object[args.size()]));
      }
      log.info(String.format("No accessible constructor with parameter types %s found", Arrays.asList(parameterTypes)));
    }
    throw new NoSuchMethodException("No accessible constructor found");
  }
}
