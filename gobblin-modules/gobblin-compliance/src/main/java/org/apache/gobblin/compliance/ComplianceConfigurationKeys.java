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
package org.apache.gobblin.compliance;

/**
 * Class containing keys for the configuration properties needed for gobblin-compliance
 *
 * @author adsharma
 */
public class ComplianceConfigurationKeys {
  public static final String COMPLIANCE_PREFIX = "gobblin.compliance";
  public static final String TRASH = "_trash_";
  public static final String BACKUP = "_backup_";
  public static final String STAGING = "_staging_";
  public static final String EXTERNAL = "EXTERNAL";
  public static final String TIMESTAMP = COMPLIANCE_PREFIX + ".job.timestamp";
  public static final String HIVE_DATASET_WHITELIST = "hive.dataset.whitelist";
  public static final String COMPLIANCE_DATASET_WHITELIST = COMPLIANCE_PREFIX + ".dataset.whitelist";
  public static final String PARTITION_NAME = COMPLIANCE_PREFIX + ".partition.name";
  public static final int TIME_STAMP_LENGTH = 13;
  public static final String DBNAME_SEPARATOR = "__";
  public static final String SPECIFY_PARTITION_FORMAT = COMPLIANCE_PREFIX + ".specifyPartitionFormat";
  public static final boolean DEFAULT_SPECIFY_PARTITION_FORMAT = false;

  public static final String NUM_ROWS = "numRows";
  public static final String RAW_DATA_SIZE = "rawDataSize";
  public static final String TOTAL_SIZE = "totalSize";

  public static final int DEFAULT_NUM_ROWS = -1;
  public static final int DEFAULT_RAW_DATA_SIZE = -1;
  public static final int DEFAULT_TOTAL_SIZE = -1;

  public static final String WORKUNIT_RECORDSREAD = COMPLIANCE_PREFIX + ".workunit.recordsRead";
  public static final String WORKUNIT_RECORDSWRITTEN = COMPLIANCE_PREFIX + ".workunit.recordsWritten";
  public static final String WORKUNIT_BYTESREAD = COMPLIANCE_PREFIX + ".workunit.bytesRead";
  public static final String WORKUNIT_BYTESWRITTEN = COMPLIANCE_PREFIX + ".workunit.bytesWritten";
  public static final String EXECUTION_COUNT = COMPLIANCE_PREFIX + ".workunit.executionCount";
  public static final String TOTAL_EXECUTIONS = COMPLIANCE_PREFIX + ".workunit.totalExecutions";
  public static final int DEFAULT_EXECUTION_COUNT = 1;

  public static final String MAX_CONCURRENT_DATASETS = COMPLIANCE_PREFIX + ".max.concurrent.datasets";
  public static final String DEFAULT_MAX_CONCURRENT_DATASETS = "100";
  public static final String HIVE_SERVER2_PROXY_USER = "hive.server2.proxy.user=";
  public static final String HIVE_JDBC_URL = COMPLIANCE_PREFIX + ".hive.jdbc.url";
  public static final String HIVE_SETTINGS = COMPLIANCE_PREFIX + ".hive.settings";

  public static final String REAPER_RETENTION_DAYS = COMPLIANCE_PREFIX + ".reaper.retention.days";
  public static final String CLEANER_BACKUP_RETENTION_VERSIONS =
      COMPLIANCE_PREFIX + ".cleaner.backup.retention.versions";
  public static final String CLEANER_BACKUP_RETENTION_DAYS = COMPLIANCE_PREFIX + ".cleaner.backup.retention.days";
  public static final String CLEANER_TRASH_RETENTION_DAYS = COMPLIANCE_PREFIX + ".cleaner.trash.retention.days";

  public static final String BACKUP_DB = COMPLIANCE_PREFIX + ".backup.db";
  public static final String BACKUP_DIR = COMPLIANCE_PREFIX + ".backup.dir";
  public static final String BACKUP_OWNER = COMPLIANCE_PREFIX + ".backup.owner";

  public static final String TRASH_DB = COMPLIANCE_PREFIX + ".trash.db";
  public static final String TRASH_DIR = COMPLIANCE_PREFIX + ".trash.dir";
  public static final String TRASH_OWNER = COMPLIANCE_PREFIX + ".trash.owner";

  public static final String HIVE_VERSIONS_WHITELIST = COMPLIANCE_PREFIX + ".hive.versions.whitelist";
  public static final String SHOULD_DROP_EMPTY_TABLES = COMPLIANCE_PREFIX + ".drop.empty.tables";
  public static final String DEFAULT_SHOULD_DROP_EMPTY_TABLES = "false";

  public static final String COMPLIANCE_JOB_SIMULATE = COMPLIANCE_PREFIX + ".simulate";
  public static final boolean DEFAULT_COMPLIANCE_JOB_SIMULATE = false;

  public static final String GOBBLIN_COMPLIANCE_JOB_CLASS = COMPLIANCE_PREFIX + ".job.class";
  public static final String GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS = COMPLIANCE_PREFIX + ".dataset.finder.class";

  public static final String GOBBLIN_COMPLIANCE_PROXY_USER = COMPLIANCE_PREFIX + ".proxy.user";
  public static final boolean GOBBLIN_COMPLIANCE_DEFAULT_SHOULD_PROXY = false;
  public static final String GOBBLIN_COMPLIANCE_SHOULD_PROXY = COMPLIANCE_PREFIX + ".should.proxy";
  public static final String GOBBLIN_COMPLIANCE_SUPER_USER = COMPLIANCE_PREFIX + ".super.user";

  public static final String RETENTION_VERSION_FINDER_CLASS_KEY = COMPLIANCE_PREFIX + ".retention.version.finder.class";

  public static final String RETENTION_SELECTION_POLICY_CLASS_KEY =
      COMPLIANCE_PREFIX + ".retention.selection.policy.class";

  public static final String DATASET_SELECTION_POLICY_CLASS = COMPLIANCE_PREFIX + ".dataset.selection.policy.class";
  public static final String DEFAULT_DATASET_SELECTION_POLICY_CLASS = "org.apache.gobblin.compliance.HivePartitionDatasetPolicy";

  public static final String PURGER_COMMIT_POLICY_CLASS = COMPLIANCE_PREFIX + ".purger.commit.policy.class";
  public static final String DEFAULT_PURGER_COMMIT_POLICY_CLASS = "org.apache.gobblin.compliance.purger.HivePurgerCommitPolicy";

  public static final String RETENTION_VERSION_CLEANER_CLASS_KEY =
      COMPLIANCE_PREFIX + ".retention.version.cleaner.class";

  public static final String RESTORE_DATASET = COMPLIANCE_PREFIX + ".restore.dataset";

  public static final String RESTORE_POLICY_CLASS = COMPLIANCE_PREFIX + ".restore.policy.class";

  public static final String DATASET_TO_RESTORE = COMPLIANCE_PREFIX + ".dataset.to.restore";

  public static final String PURGE_POLICY_CLASS = COMPLIANCE_PREFIX + ".purge.policy.class";

  public static final String VALIDATION_POLICY_CLASS = COMPLIANCE_PREFIX + ".validation.policy.class";

  // Name of the column in the compliance id table containing compliance id.
  public static final String COMPLIANCEID_KEY = COMPLIANCE_PREFIX + ".purger.complianceId";
  // Path to the compliance id in the dataset descriptor json object.
  public static final String DATASET_DESCRIPTOR_FIELDPATH = COMPLIANCE_PREFIX + ".dataset.descriptor.fieldPath";
  public static final String DATASET_DESCRIPTOR_CLASS = COMPLIANCE_PREFIX + ".dataset.descriptor.class";
  public static final String DEFAULT_DATASET_DESCRIPTOR_CLASS = "org.apache.gobblin.compliance.DatasetDescriptorImpl";

  // Name of the table containing the compliance ids based on which purging will take place.
  public static final String COMPLIANCE_ID_TABLE_KEY = COMPLIANCE_PREFIX + ".purger.complianceIdTable";

  /**
   * Configuration keys for the dataset descriptor which will be present in tblproperties of a Hive table.
   */
  public static final String DATASET_DESCRIPTOR_KEY = "dataset.descriptor";
  public static final String HIVE_PURGER_WATERMARK = COMPLIANCE_PREFIX + ".purger.watermark";
  public static final String NO_PREVIOUS_WATERMARK = COMPLIANCE_PREFIX + ".purger.noWatermark";

  public static final String MAX_WORKUNITS_KEY = COMPLIANCE_PREFIX + ".purger.maxWorkunits";

  /**
   * Configuration keys for the execution attempts of a work unit.
   */
  public static final String EXECUTION_ATTEMPTS = COMPLIANCE_PREFIX + ".workunits.executionAttempts";
  public static final String MAX_WORKUNIT_EXECUTION_ATTEMPTS_KEY =
      COMPLIANCE_PREFIX + ".workunits.maxExecutionAttempts";
  public static final int DEFAULT_EXECUTION_ATTEMPTS = 1;
  public static final int DEFAULT_MAX_WORKUNIT_EXECUTION_ATTEMPTS = 3;
  public static final int DEFAULT_MAX_WORKUNITS = 5;
}
