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
package org.apache.gobblin.audit.values.auditor;

import java.util.List;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.apache.avro.Schema;


/**
 * A container for table specific runtime Metadata required for auditing a table.
 * Use {@link ValueAuditRuntimeMetadataBuilder} to instantiate a new {@link ValueAuditRuntimeMetadata}.
 * <code>database, table and tableSchema</code> are required fields in the {@link ValueAuditRuntimeMetadata}.
 * All other fields <code>phase, cluster, extractId, snapshotId, deltaId, partFileName</code> are marked as {@value #DEFAULT_VALUE}
 * if not present.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@ToString
@EqualsAndHashCode
public class ValueAuditRuntimeMetadata {

  private static final String DEFAULT_VALUE = "NA";

  /**
   * <i>Required - </i>Table specific metadata like <code>database, table and tableSchema</code>
   */
  private TableMetadata tableMetadata;
  /**
   * <i>Optional - </i>The snapshot generation phase being audited
   */
  private Phase phase;
  /**
   * <i>Optional - </i>Audited snapshot data's cluster
   */
  private String cluster;
  /**
   * <i>Optional - </i>Extract Id of the snapshot
   */
  private String extractId;
  /**
   * <i>Optional - </i>Snapshot Id being audited
   */
  private String snapshotId;
  /**
   * <i>Optional - </i>Delta Id of the snapshot
   */
  private String deltaId;
  /**
   * <i>Optional - </i>Part file in the snapshot being audited
   */
  private String partFileName;

  public static ValueAuditRuntimeMetadataBuilder builder(String databaseName, String tableName, Schema tableSchema) {
    return new ValueAuditRuntimeMetadataBuilder(databaseName, tableName, tableSchema);
  }

  /**
   * Container for table specific metadata
   */
  @Getter
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public static class TableMetadata {
    /**
     * <i>Required - </i> database name
     */
    @NonNull private String database;
    /**
     * <i>Required - </i> table name
     */
    @NonNull private String table;
    /**
     * <i>Required - </i> table schema
     */
    @NonNull private Schema tableSchema;
    /**
     * <i>Optional - </i> list of key fields in the table that uniquely identify a row.
     * Each entry in the list is an ordered string specifying the location of the nested key field
     * For example, field1.nestedField1 refers to the field "nestedField1" inside of field "field1" of the record.
     */
    private List<String> keyFieldLocations;
    /**
     * <i>Optional - </i> list of delta fields in the table that are used to track changes in the row over time.
     * Each entry in the list is an ordered string specifying the location of the nested delta field
     * For example, field1.nestedField1 refers to the field "nestedField1" inside of field "field1" of the record.
     */
    private List<String> deltaFieldLocations;
  }

  /**
   * An enum for all phases snapshot generation
   */
  public static enum Phase {
    PULL("Pull from extract"),
    AVRO_CONV("Convert to avro"),
    SS_GEN("Snapshot Generation, LSB"),
    SS_UPD("Snapshot update, VSB"),
    SS_MAT("Snapshot materialization, QSB"),
    SS_PUB("Publish Snapshot"),
    NA("Not Applicable");

    private String description;

    Phase(String description) {
      this.description = description;
    }

    public String getDescription() {
      return this.description;
    }
  }

  /**
   * Builder to build A {@link ValueAuditRuntimeMetadata}, <code>databaseName, tableName and tableSchema</code> are required
   */
  public static class ValueAuditRuntimeMetadataBuilder {

    private TableMetadata tableMetadata;
    private Phase phase = Phase.NA;
    private String cluster = DEFAULT_VALUE;
    private String extractId = DEFAULT_VALUE;
    private String snapshotId = DEFAULT_VALUE;
    private String deltaId = DEFAULT_VALUE;
    private String partFileName = DEFAULT_VALUE;

    public ValueAuditRuntimeMetadataBuilder(String databaseName, String tableName, Schema tableSchema) {
      this.tableMetadata = new TableMetadata(databaseName, tableName, tableSchema);
    }

    public ValueAuditRuntimeMetadataBuilder phase(final Phase phase) {
      this.phase = phase;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder cluster(final String cluster) {
      this.cluster = cluster;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder extractId(final String extractId) {
      this.extractId = extractId;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder snapshotId(final String snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder deltaId(final String deltaId) {
      this.deltaId = deltaId;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder partFileName(final String partFileName) {
      this.partFileName = partFileName;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder tableMetadataKeyFieldLocations(List<String> keyFieldLocations) {
      this.tableMetadata.keyFieldLocations = keyFieldLocations;
      return this;
    }

    public ValueAuditRuntimeMetadataBuilder tableMetadataDeltaFieldLocations(List<String> deltaFieldLocations) {
      this.tableMetadata.keyFieldLocations = deltaFieldLocations;
      return this;
    }

    public ValueAuditRuntimeMetadata build() {
      return new ValueAuditRuntimeMetadata(tableMetadata, phase, cluster, extractId, snapshotId, deltaId, partFileName);
    }
  }
}
