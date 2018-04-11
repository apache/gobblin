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

package org.apache.gobblin.broker;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link ResourceEntry} that expires immediately. The resource is not closed on invalidation since the lifetime of
 * the object cannot be determined by the cache, so the recipient of the resource needs to close it.
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class ImmediatelyInvalidResourceEntry<T> extends ResourceInstance<T> {
  private boolean valid;

  public ImmediatelyInvalidResourceEntry(T resource) {
    super(resource);
    this.valid = true;
  }

  @Override
  public synchronized T getResource() {
    // mark the object as invalid before returning so that a new one will be created on the next
    // request from the factory
    this.valid = false;

    return super.getResource();
  }

  @Override
  public boolean isValid() {
    return this.valid;
  }

  @Override
  public void onInvalidate() {
    // these type of resource cannot be closed on invalidation since the lifetime can't be determined
  }

  /**
   * This method is synchronized so that the validity check and validity change is atomic for callers of this method.
   * @return
   */
  @Override
  public synchronized T getResourceIfValid() {
    if (this.valid) {
      return getResource();
    } else {
      return null;
    }
  }
}
