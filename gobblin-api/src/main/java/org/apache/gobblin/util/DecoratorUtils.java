/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util;

import java.util.List;

import com.google.common.collect.Lists;


/**
 * Utilities for classes implementing the decorator interface.
 */
public class DecoratorUtils {

  /**
   * Resolves the truly underlying object in a possible chain of {@link org.apache.gobblin.util.Decorator}.
   *
   * @param obj object to resolve.
   * @return the true non-decorator underlying object.
   */
  public static Object resolveUnderlyingObject(Object obj) {
    while(obj instanceof Decorator) {
      obj = ((Decorator)obj).getDecoratedObject();
    }
    return obj;
  }

  /**
   * Finds the decorator lineage of the given object.
   *
   * <p>
   * If object is not a {@link org.apache.gobblin.util.Decorator}, this method will return a singleton list with just the object.
   * If object is a {@link org.apache.gobblin.util.Decorator}, it will return a list of the underlying object followed by the
   * decorator lineage up to the input decorator object.
   * </p>
   *
   * @param obj an object.
   * @return List of the non-decorator underlying object and all decorators on top of it,
   *  starting with underlying object and ending with the input object itself (inclusive).
   */
  public static List<Object> getDecoratorLineage(Object obj) {
    List<Object> lineage = Lists.newArrayList(obj);
    Object currentObject = obj;
    while(currentObject instanceof Decorator) {
      currentObject = ((Decorator)currentObject).getDecoratedObject();
      lineage.add(currentObject);
    }

    return Lists.reverse(lineage);
  }
}
