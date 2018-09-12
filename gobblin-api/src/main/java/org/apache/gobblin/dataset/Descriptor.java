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

package org.apache.gobblin.dataset;

import com.google.gson.Gson;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A descriptor is a simplified representation of a resource, which could be a dataset, dataset partition, file, etc.
 * It is a digest or abstract, which contains identification properties of the actual resource object, such as ID, name
 * primary keys, version, etc
 *
 * <p>
 *   When the original object has complicated inner structure and there is a requirement to send it over the wire,
 *   it's a time to define a corresponding {@link Descriptor} becomes. In this case, the {@link Descriptor} can just
 *   have minimal information enough to construct the original object on the other side of the wire
 * </p>
 *
 * <p>
 *   When the cost to instantiate the complete original object is high, for example, network calls are required, but
 *   the use cases are limited to the identification properties, define a corresponding {@link Descriptor} becomes
 *   handy
 * </p>
 */
@RequiredArgsConstructor
public class Descriptor {

  /** Use gson for ser/de */
  private static final Gson GSON = new Gson();

  /** Name of the resource */
  @Getter
  private final String name;

  @Override
  public String toString() {
    return GSON.toJson(this);
  }

  public Descriptor copy() {
    return new Descriptor(name);
  }
}
