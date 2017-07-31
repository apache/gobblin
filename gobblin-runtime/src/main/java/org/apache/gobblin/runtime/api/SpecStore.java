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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;


public interface SpecStore {

  /***
   * Check if a {@link Spec} exists in {@link SpecStore} by URI.
   * @param specUri URI for the {@link Spec} to be checked.
   * @throws IOException Exception in checking if {@link Spec} exists.
   */
  boolean exists(URI specUri) throws IOException;

  /***
   * Persist {@link Spec} in the {@link SpecStore} for durability.
   * @param spec {@link Spec} to be persisted.
   * @throws IOException Exception in persisting.
   */
  void addSpec(Spec spec) throws IOException;

  /***
   * Delete {@link Spec} from the {@link SpecStore}.
   * If {@link Spec} is not found, it is a no-op.
   * @param spec {@link Spec} to be deleted.
   * @return true if {@link Spec} was deleted else false.
   * @throws IOException Exception in deleting.
   */
  boolean deleteSpec(Spec spec) throws IOException;

  /***
   * Delete all versions of the {@link Spec} from the {@link SpecStore}.
   * If {@link Spec} is not found, it is a no-op.
   * @param specUri URI for the {@link Spec} to be deleted.
   * @return true if {@link Spec} was deleted else false.
   * @throws IOException Exception in deleting.
   */
  boolean deleteSpec(URI specUri) throws IOException;

  /***
   * Delete specifid version of {@link Spec} from the {@link SpecStore}.
   * If {@link Spec} is not found, it is a no-op.
   * @param specUri URI for the {@link Spec} to be deleted.
   * @param version Version for the {@link Spec} to be deleted.
   * @return true if {@link Spec} was deleted else false.
   * @throws IOException Exception in deleting.
   */
  boolean deleteSpec(URI specUri, String version) throws IOException;

  /***
   * Update {@link Spec} in the {@link SpecStore}.
   * @param spec {@link Spec} to be updated.
   * @throws IOException Exception in updating the {@link Spec}.
   * @return Updated {@link Spec}.
   * @throws SpecNotFoundException If {@link Spec} being updated is not present in store.
   */
  Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException;

  /***
   * Retrieve the latest version of the {@link Spec} by URI from the {@link SpecStore}.
   * @param specUri URI for the {@link Spec} to be retrieved.
   * @throws IOException Exception in retrieving the {@link Spec}.
   * @throws SpecNotFoundException If {@link Spec} being retrieved is not present in store.
   */
  Spec getSpec(URI specUri) throws IOException, SpecNotFoundException;

  /***
   * Retrieve specified version of the {@link Spec} by URI from the {@link SpecStore}.
   * @param specUri URI for the {@link Spec} to be retrieved.
   * @param version Version for the {@link Spec} to be retrieved.
   * @throws IOException Exception in retrieving the {@link Spec}.
   * @throws SpecNotFoundException If {@link Spec} being retrieved is not present in store.
   */
  Spec getSpec(URI specUri, String version) throws IOException, SpecNotFoundException;

  /***
   * Retrieve all versions of the {@link Spec} by URI from the {@link SpecStore}.
   * @param specUri URI for the {@link Spec} to be retrieved.
   * @throws IOException Exception in retrieving the {@link Spec}.
   * @throws SpecNotFoundException If {@link Spec} being retrieved is not present in store.
   */
  Collection<Spec> getAllVersionsOfSpec(URI specUri) throws IOException, SpecNotFoundException;

  /***
   * Get all {@link Spec}s from the {@link SpecStore}.
   * @throws IOException Exception in retrieving {@link Spec}s.
   */
  Collection<Spec> getSpecs() throws IOException;
}
