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

package org.apache.gobblin.util.schema_check;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * The strategy to compare Avro schema.
 */
public interface AvroSchemaCheckStrategy {
  /**
   * A factory to initiate the Strategy
   */
  @Slf4j
  class AvroSchemaCheckStrategyFactory {
    /**
     * Use the configuration to create a schema check strategy. If it's not found, return null.
     * @param state
     * @return
     */
    public static AvroSchemaCheckStrategy create(WorkUnitState state)
    {
      try {
        return (AvroSchemaCheckStrategy) Class.forName(state.getProp(ConfigurationKeys.AVRO_SCHEMA_CHECK_STRATEGY, ConfigurationKeys.AVRO_SCHEMA_CHECK_STRATEGY_DEFAULT)).newInstance();
      } catch (Exception e) {
        log.error(e.getMessage());
        return null;
      }
    }
  }
  /**
   * Make sure schema toValidate and expected have matching names and types.
   * @param toValidate The real schema
   * @param expected The expected schema
   * @return
   */
  boolean compare(Schema expected, Schema toValidate);
}
