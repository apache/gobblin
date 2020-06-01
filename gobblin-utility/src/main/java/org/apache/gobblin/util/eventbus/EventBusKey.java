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

package org.apache.gobblin.util.eventbus;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.broker.iface.SharedResourceKey;

@EqualsAndHashCode
@Getter
public class EventBusKey implements SharedResourceKey{
  private final String sourceClassName;

  public EventBusKey(String sourceClassName) {
    this.sourceClassName = sourceClassName;
  }

  /**
   * @return A serialization of the {@link SharedResourceKey} into a short, sanitized string. Users configure a
   *         shared resource using the value of this method.
   */
  @Override
  public String toConfigurationKey() {
    return this.sourceClassName;
  }
}
