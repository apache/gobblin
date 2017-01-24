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

import com.google.common.base.Joiner;

import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceKey;

import javax.annotation.Nonnull;
import lombok.Builder;


/**
 * Generates key strings that the default {@link gobblin.broker.iface.SharedResourcesBroker} can understand. Useful
 * for populating a configuration programmatically.
 */
public class BrokerConfigurationKeyGenerator {

  private static final Joiner JOINER = Joiner.on(".").skipNulls();

  /**
   * Generate a {@link gobblin.broker.iface.SharedResourcesBroker} configuration key for a particular {@link SharedResourceFactory},
   * {@link SharedResourceKey} and {@link ScopeType}.
   *
   * Example:
   * If the broker configuration contains a key-value pair with key:
   * generateKey(myFactory, myKey, myScopeType, "sample.key")
   * when requesting a resource created by myFactory, with the provided key and scope, the factory will be able to see
   * the key-value pair specified.
   *
   * Note:
   * {@link SharedResourceKey} and {@link ScopeType} may be null. In this case, the key-value pair will be visible to
   * the factory regardless of the key and scope requested by the user.
   */
  @Builder
  public static String generateKey(@Nonnull SharedResourceFactory factory, SharedResourceKey key, ScopeType scopeType,
      @Nonnull String configKey) {
    return JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, factory.getName(), scopeType == null ? null : scopeType.name(),
        key == null ? null : key.toConfigurationKey(), configKey);
  }

}
