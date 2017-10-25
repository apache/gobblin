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

package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.Collection;


/**
 * A {@link SpecCatalog} that can have its {@link Collection} of {@link Spec}s modified
 * programmatically. Note that specs in a spec catalog can change from the outside. This is covered
 * by the base SpecCatalog interface.
 */
public interface MutableSpecCatalog extends SpecCatalog {
  /**
   * Registers a new {@link Spec}. If a {@link Spec} with the same {@link Spec#getUri()} exists,
   * it will be replaced.
   * */
  public void put(Spec spec);

  /**
   * Removes an existing {@link Spec} with the given URI.
   * Throws SpecNotFoundException if such {@link Spec} does not exist.
   */
  void remove(URI uri) throws SpecNotFoundException;
}
