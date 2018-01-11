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
package org.apache.gobblin.capability;

import java.util.Map;

import org.apache.gobblin.annotation.Alpha;

/**
 * Describes an object that is aware of the capabilities it supports.
 */
@Alpha
public interface CapabilityAware {
  /**
   * Checks if this object supports the given Capability with the given properties.
   *
   * Implementers of this should always check if their super-class may happen to support a capability
   * before returning false!
   * @param c Capability being queried
   * @param properties Properties specific to the capability. Properties are capability specific.
   * @return True if this object supports the given capability + property settings, false if not
   */
  boolean supportsCapability(Capability c, Map<String, Object> properties);
}
