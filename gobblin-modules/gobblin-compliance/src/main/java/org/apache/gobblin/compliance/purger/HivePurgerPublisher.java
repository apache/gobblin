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
package gobblin.compliance.purger;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.ComplianceEvents;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.utils.DatasetUtils;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.publisher.DataPublisher;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HostUtils;


/**
 * The Publisher moves COMMITTED WorkUnitState to SUCCESSFUL, otherwise FAILED.
 *
 * @author adsharma
 */
@Slf4j
public class HivePurgerPublisher extends DataPublisher {
  protected MetricContext metricContext;
  protected EventSubmitter eventSubmitter;
  public HiveMetaStoreClient client;

  public HivePurgerPublisher(State state) throws Exception {
    super(state);
    this.metricContext = Instrumented.getMetricContext(state, this.getClass());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, ComplianceEvents.NAMESPACE).
        build();

    initHiveMetastoreClient();
  }

  public void initHiveMetastoreClient() throws Exception {
    if (this.state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION)) {
      String superUser = this.state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
      String realm = this.state.getProp(ConfigurationKeys.KERBEROS_REALM);
      String keytabLocation = this.state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
      log.info("Establishing MetastoreClient connection using " + keytabLocation);

      UserGroupInformation.loginUserFromKeytab(HostUtils.getPrincipalUsingHostname(superUser, realm), keytabLocation);
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      loginUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws TException {
          HivePurgerPublisher.this.client = new HiveMetaStoreClient(new HiveConf());
          return null;
        }
      });
    } else {
      HivePurgerPublisher.this.client = new HiveMetaStoreClient(new HiveConf());
    }
  }

  public void initialize() {
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) {
    for (WorkUnitState state : states) {
      if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        submitEvent(state, ComplianceEvents.Purger.WORKUNIT_COMMITTED);
      } else {
        state.setWorkingState(WorkUnitState.WorkingState.FAILED);
        submitEvent(state, ComplianceEvents.Purger.WORKUNIT_FAILED);
      }
    }
  }

  private void submitEvent(WorkUnitState state, String name) {
    WorkUnit workUnit = state.getWorkunit();
    Map<String, String> metadata = new HashMap<>();
    String recordsRead = state.getProp(ComplianceConfigurationKeys.NUM_ROWS);
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_RECORDSREAD, recordsRead);
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_BYTESREAD,
        getDataSize(workUnit.getProp(ComplianceConfigurationKeys.RAW_DATA_SIZE),
            workUnit.getProp(ComplianceConfigurationKeys.TOTAL_SIZE)));

    String partitionNameProp = workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME);
    Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();
    List<String> namesList = AT_SPLITTER.splitToList(partitionNameProp);
    if (namesList.size() != 3) {
      log.warn("Not submitting event. Invalid partition name: " + partitionNameProp);
      return;
    }

    String dbName = namesList.get(0), tableName = namesList.get(1), partitionName = namesList.get(2);
    org.apache.hadoop.hive.metastore.api.Partition apiPartition = null;
    Partition qlPartition = null;
    try {
      Table table = new Table(this.client.getTable(dbName, tableName));
      apiPartition = this.client.getPartition(dbName, tableName, partitionName);
      qlPartition = new Partition(table, apiPartition);
    } catch (Exception e) {
      log.warn("Not submitting event. Failed to resolve partition '" + partitionName + "': " + e);
      e.printStackTrace();
      return;
    }

    HivePartitionDataset hivePartitionDataset = new HivePartitionDataset(qlPartition);

    String recordsWritten = DatasetUtils.getProperty(hivePartitionDataset, ComplianceConfigurationKeys.NUM_ROWS,
        ComplianceConfigurationKeys.DEFAULT_NUM_ROWS);

    String recordsPurged = Long.toString((Long.parseLong(recordsRead) - Long.parseLong(recordsWritten)));
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_RECORDSWRITTEN,
        recordsWritten);
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_BYTESWRITTEN, getDataSize(DatasetUtils
        .getProperty(hivePartitionDataset, ComplianceConfigurationKeys.RAW_DATA_SIZE,
            ComplianceConfigurationKeys.DEFAULT_RAW_DATA_SIZE), DatasetUtils
        .getProperty(hivePartitionDataset, ComplianceConfigurationKeys.TOTAL_SIZE,
            ComplianceConfigurationKeys.DEFAULT_TOTAL_SIZE)));

    metadata.put(DatasetMetrics.DATABASE_NAME, hivePartitionDataset.getDbName());
    metadata.put(DatasetMetrics.TABLE_NAME, hivePartitionDataset.getTableName());
    metadata.put(DatasetMetrics.PARTITION_NAME, hivePartitionDataset.getName());
    metadata.put(DatasetMetrics.RECORDS_PURGED, recordsPurged);

    this.eventSubmitter.submit(name, metadata);
  }

  private String getDataSize(String rawDataSize, String totalDataSize) {
    long rawDataSizeVal = Long.parseLong(rawDataSize);
    long totalDataSizeVal = Long.parseLong(totalDataSize);
    long dataSize = totalDataSizeVal;
    if (totalDataSizeVal <= 0) {
      dataSize = rawDataSizeVal;
    }
    return Long.toString(dataSize);
  }

  public void publishMetadata(Collection<? extends WorkUnitState> states) {
  }

  @Override
  public void close() {
  }

  public static class DatasetMetrics {
    public static final String DATABASE_NAME = "HiveDatabaseName";
    public static final String TABLE_NAME = "HiveTableName";
    public static final String PARTITION_NAME = "HivePartitionName";
    public static final String RECORDS_PURGED = "RecordsPurged";
  }
}
