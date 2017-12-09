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

import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import lombok.Data;


/**
 * A {@link SharedResourceFactoryResponse} that returns a newly created resource instance.
 */
@Data
public class ResourceInstance<T> implements ResourceEntry<T> {
  // Note: the name here is theResource instead of resource since to avoid a collision of the lombok generated getter
  // and the getResource() method defined in {@link ResourceEntry}. The collision results in unintended side effects
  // when getResource() is overridden since it may have additional logic that should not be executed when the value of
  // this field is fetched using the getter, such as in the Lombok generated toString().
  private final T theResource;

  /**
   * This method returns the resource, but may have logic before the return.
   * @return the resource
   */
  @Override
  public T getResource() {
    return getTheResource();
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public void onInvalidate() {
    // this should never happen
    throw new RuntimeException();
  }
}
