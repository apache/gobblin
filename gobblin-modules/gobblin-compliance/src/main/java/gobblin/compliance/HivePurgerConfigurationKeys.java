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
package gobblin.compliance;

/**
 * A central place for all configuration property keys required for purging Hive tables.
 *
 * @author adsharma
 */
public class HivePurgerConfigurationKeys {

  public static final String HIVE_PURGER_PREFIX = "hive.purger";
  public static final String STAGING_PREFIX = "staging_";
  public static final String EXTERNAL = "EXTERNAL";
  public static final String PARTITION_NAME = HIVE_PURGER_PREFIX + ".partition.name";
  public static final String HIVE_DATASET_WHITELIST = "hive.dataset.whitelist";
  public static final String HIVE_PURGER_JOB_TIMESTAMP = HIVE_PURGER_PREFIX + ".job.timestamp";

  public static final String COMMIT_KEY = HIVE_PURGER_PREFIX + ".commit";
  public static final Boolean DEFAULT_COMMIT = true;

  // Name of the directory for writing staging data.
  public static final String STAGING_DIR_KEY = HIVE_PURGER_PREFIX + ".staging.dir";

  // Name of the db for creating staging tables.
  public static final String STAGING_DB_KEY = HIVE_PURGER_PREFIX + ".staging.db";

  public static final String HIVE_PURGER_WATERMARK = HIVE_PURGER_PREFIX + ".watermark";
  public static final String MAX_WORKUNITS_KEY = HIVE_PURGER_PREFIX + ".maxWorkunits";

  public static final String NO_PREVIOUS_WATERMARK = HIVE_PURGER_PREFIX + ".noWatermark";

  /**
   * Configuration keys for the execution attempts of a work unit.
   */
  public static final String EXECUTION_ATTEMPTS = HIVE_PURGER_PREFIX + "execution.attempts";
  public static final String MAX_WORKUNIT_EXECUTION_ATTEMPTS_KEY = HIVE_PURGER_PREFIX + ".maxExecutionAttempts";
  public static final int DEFAULT_EXECUTION_ATTEMPTS = 1;
  public static final int DEFAULT_MAX_WORKUNIT_EXECUTION_ATTEMPTS = 3;
  public static final int DEFAULT_MAX_WORKUNITS = 5;

  public static final String HIVE_SOURCE_DATASET_FINDER_CLASS_KEY = "hive.dataset.finder.class";

  /**
   * Configuration keys for the dataset descriptor which will be present in tblproperties of a Hive table.
   */
  public static final String DATASET_DESCRIPTOR_KEY = "dataset.descriptor";

  // Path to the compliance id in the dataset descriptor json object.
  public static final String DATASET_DESCRIPTOR_IDENTIFIER = "dataset.descriptor.identifier";
  public static final String DATASET_DESCRIPTOR_CLASS = "dataset.descriptor.class";
  public static final String DEFAULT_DATASET_DESCRIPTOR_CLASS = "gobblin.compliance.DatasetDescriptorImpl";

  // Name of the table containing the compliance ids based on which purging will take place.
  public static final String COMPLIANCE_ID_TABLE_KEY = HIVE_PURGER_PREFIX + ".complianceId.table";

  // Name of the column in the compliance id table containing compliance id.
  public static final String COMPLIANCE_IDENTIFIER_KEY = HIVE_PURGER_PREFIX + ".compliance.identifier";
}
