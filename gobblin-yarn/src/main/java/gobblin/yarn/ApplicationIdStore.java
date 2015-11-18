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

package gobblin.yarn;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Optional;


/**
 * An interface for classes that implement a persistent store for a single Yarn
 * {@link org.apache.hadoop.yarn.api.records.ApplicationId} in its string form.
 *
 * <p>
 *   This is useful in {@link GobblinYarnAppLauncher} to retrieve and persist
 *   the application ID of the Yarn application it launches so it is able to
 *   reconnect to the application upon restart.
 * </p>
 *
 * @author ynli
 */
interface ApplicationIdStore extends Closeable {

  /**
   * Put a given application ID into the store.
   *
   * <p>
   *   If an application ID already exists in the store, it will be replaced with the new ID.
   * </p>
   *
   * @param applicationId the application ID to store.
   * @throws IOException if it fails to put the given application ID into the store.
   */
  void put(String applicationId) throws IOException;

  /**
   * Get the current application ID in the store.
   *
   * @return an {@link Optional} that wraps the application ID if it exists in the store, or
   *         an {@link Optional} that is not present.
   * @throws IOException if it fails to get the given application ID from the store.
   */
  Optional<String> get() throws IOException;

  /**
   * Delete the current application ID in the store.
   *
   * @throws IOException if it fails to delete the current application ID in the store.
   */
  void delete() throws IOException;
}
