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


/**
 * Interface for decorator patterns.
 */
public interface Decorator {

  /**
   * Gets underlying object that is being decorated.
   *
   * <p>
   *   Since the directly underlying object itself may be a decorator, the implementation should make sure to resolve
   *   the actual underlying object.
   * </p>
   *
   * <p>
   *   A static implementation usable from any Decorator is available at
   *   {@link gobblin.util.DecoratorUtils#resolveUnderlyingObject}.
   * </p>
   * @return Underlying object.
   */
  public Object getUnderlying();

  /**
   * Gets list of instances of all decorators and the underlying object.
   *
   * <p>
   *   The directly underlying object may itself be a decorator. This method is used to get the entire lineage
   *   of decorators up to the truly underlying object.
   * </p>
   *
   * <p>
   *    For example, if decorator A decorates decorator B, which decorates Object C, then
   *    A.getDecoratorLineage() should return [C, B, A], while B.getDecoratorLineage()
   *    should return [C, B].
   * </p>
   *
   * <p>
   *   A static implementation usable from any Decorator is available at
   *   {@link gobblin.util.DecoratorUtils#resolveDecoratorLineage}.
   * </p>
   * @return lineage of decorators and underlying object.
   */
  public List<Object> getDecoratorLineage();

}
