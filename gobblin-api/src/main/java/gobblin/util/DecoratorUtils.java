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
   * <p>
   *   If underlyingCandidate is a decorator, return underlyingCandidate.getUnderlying().
   *   Otherwise, return underlyingCandidate itself.
   * </p>
   *
   * @param underlyingCandidate the directly overlying object in a Decorator.
   * @return the true non-decorator underlying object.
   */
  public static Object resolveUnderlyingObject(Object underlyingCandidate) {
    if (underlyingCandidate instanceof Decorator) {
      return ((Decorator)underlyingCandidate).getUnderlying();
    } else {
      return underlyingCandidate;
    }
  }

  /**
   * Finds the decorator lineage of the given decorator with the given directly underlying object.
   *
   * @param decorator decorator object.
   * @param underlying directly underlying object to decorator.
   * @return List of the non-decorator underlying object and all decorators on top of it.
   */
  public static List<Object> resolveDecoratorLineage(Decorator decorator, Object underlying) {
    List<Object> lineage =  Decorator.class.isInstance(underlying) ?
        ((Decorator)underlying).getDecoratorLineage() :
        Lists.newArrayList(underlying);
    lineage.add(decorator);

    return lineage;
  }
}
