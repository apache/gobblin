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

package gobblin.compaction.suite;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ClassAliasResolver;

/**
 * A utility class for {@link CompactionSuite}
 */
public class CompactionSuiteUtils {

  /**
   * Return an {@link CompactionSuiteFactory} based on the configuration
   * @return A concrete suite factory instance. By default {@link CompactionAvroSuiteFactory} is used.
   */
  public static CompactionSuiteFactory getCompactionSuiteFactory (State state) {
    try {
      String factoryName = state.getProp(ConfigurationKeys.COMPACTION_SUITE_FACTORY, ConfigurationKeys.DEFAULT_COMPACTION_SUITE_FACTORY);

      ClassAliasResolver<CompactionSuiteFactory> conditionClassAliasResolver = new ClassAliasResolver<>(CompactionSuiteFactory.class);
      CompactionSuiteFactory factory = conditionClassAliasResolver.resolveClass(factoryName).newInstance();
      return factory;
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
