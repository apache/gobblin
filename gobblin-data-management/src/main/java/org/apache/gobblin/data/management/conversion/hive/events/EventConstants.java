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
package org.apache.gobblin.data.management.conversion.hive.events;

import org.apache.gobblin.metrics.event.sla.SlaEventKeys;

/**
 * Event names and metadata names used by hive conversion.
 */
public class EventConstants {

  public static final String CONVERSION_NAMESPACE = "gobblin.hive.conversion";
  public static final String VALIDATION_NAMESPACE = "gobblin.hive.validation";
  public static final String CONVERSION_PREFIX = CONVERSION_NAMESPACE + ".";
  public static final String VALIDATION_PREFIX = VALIDATION_NAMESPACE + ".";

  //Event names
  public static final String CONVERSION_SETUP_EVENT = CONVERSION_PREFIX + "Setup";
  public static final String CONVERSION_FIND_HIVE_TABLES_EVENT = CONVERSION_PREFIX + "FindHiveTables";
  public static final String CONVERSION_SUCCESSFUL_SLA_EVENT = CONVERSION_PREFIX + "ConversionSuccessful";
  public static final String CONVERSION_FAILED_EVENT = CONVERSION_PREFIX + "ConversionFailed";

  //Event names
  public static final String VALIDATION_SETUP_EVENT = VALIDATION_PREFIX + "Setup";
  public static final String VALIDATION_FIND_HIVE_TABLES_EVENT = VALIDATION_PREFIX + "FindHiveTables";
  public static final String VALIDATION_SUCCESSFUL_EVENT = VALIDATION_PREFIX + "ValidationSuccessful";
  public static final String VALIDATION_FAILED_EVENT = VALIDATION_PREFIX + "ValidationFailed";
  public static final String VALIDATION_NOOP_EVENT = VALIDATION_PREFIX + "ValidationNoop";

  // Event metadata keys
  // The final event metadata name will be SchemaEvolutionDDLNum as SlaEventSubmitter removes the prefix
  // SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX
  public static final String SCHEMA_EVOLUTION_DDLS_NUM = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "schemaEvolutionDDLNum";
  public static final String BEGIN_DDL_BUILD_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "beginDDLBuildTime";
  public static final String END_DDL_BUILD_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "endDDLBuildTime";
  public static final String BEGIN_CONVERSION_DDL_EXECUTE_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "beginConversionDDLExecuteTime";
  public static final String END_CONVERSION_DDL_EXECUTE_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "endConversionDDLExecuteTime";
  public static final String BEGIN_PUBLISH_DDL_EXECUTE_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "beginPublishDDLExecuteTime";
  public static final String END_PUBLISH_DDL_EXECUTE_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "endPublishDDLExecuteTime";
  public static final String WORK_UNIT_CREATE_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "workunitCreateTime";
  public static final String BEGIN_GET_WORKUNITS_TIME = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "beginGetWorkunitsTime";

  public static final String SOURCE_DATA_LOCATION = SlaEventKeys.EVENT_ADDITIONAL_METADATA_PREFIX + "sourceDataLocation";
}
