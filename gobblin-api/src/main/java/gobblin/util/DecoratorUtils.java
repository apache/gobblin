/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.util.List;

import com.google.common.collect.Lists;


/**
 * Utilities for classes implementing the decorator interface.
 */
public class DecoratorUtils {

  /**
   * Resolves the truly underlying object in a possible chain of {@link gobblin.util.Decorator}.
   *
   * @param obj object to resolve.
   * @return the true non-decorator underlying object.
   */
  public static Object resolveUnderlyingObject(Object obj) {
    Object underlying = obj;
    while(underlying instanceof Decorator) {
      underlying = ((Decorator)underlying).getDirectlyUnderlying();
    }
    return underlying;
  }

  /**
   * Finds the decorator lineage of the given decorator with the given directly underlying object.
   *
   * @param obj decorator object.
   * @return List of the non-decorator underlying object and all decorators on top of it,
   *  starting with underlying object and ending with decorator.
   */
  public static List<Object> getDecoratorLineage(Object obj) {
    List<Object> lineage = Lists.newArrayList(obj);
    Object currentObject = obj;
    while(currentObject instanceof Decorator) {
      currentObject = ((Decorator)currentObject).getDirectlyUnderlying();
      lineage.add(currentObject);
    }

    return Lists.reverse(lineage);
  }
}
