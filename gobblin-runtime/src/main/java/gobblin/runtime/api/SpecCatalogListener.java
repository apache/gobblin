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

package gobblin.runtime.api;

import java.net.URI;


public interface SpecCatalogListener {
  /** Invoked when a new {@link Spec} is added to the catalog and for all pre-existing specs on registration
   * of the listener.*/
  void onAddSpec(Spec addedSpec);

  /**
   * Invoked when a {@link Spec} gets removed from the catalog.
   */
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion);

  /**
   * Invoked when the contents of a {@link Spec} gets updated in the catalog.
   */
  public void onUpdateSpec(Spec updatedSpec);
}
