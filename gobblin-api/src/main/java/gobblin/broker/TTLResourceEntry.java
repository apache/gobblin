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

package gobblin.broker;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link ResourceEntry} that automatically expires after a given number of milliseconds.
 */
@Slf4j
public class TTLResourceEntry<T> implements ResourceEntry<T> {
  @Getter
  private final T resource;
  private final long expireAt;
  private final boolean closeOnInvalidation;

  public TTLResourceEntry(T resource, long millisToLive, boolean closeOnInvalidation) {
    this.resource = resource;
    this.expireAt = System.currentTimeMillis() + millisToLive;
    this.closeOnInvalidation = closeOnInvalidation;
  }

  @Override
  public boolean isValid() {
    return System.currentTimeMillis() < this.expireAt;
  }

  @Override
  public void onInvalidate() {
    if (this.closeOnInvalidation) {
      ResourceEntry.shutdownObject(this.resource, log);
    }
  }
}
