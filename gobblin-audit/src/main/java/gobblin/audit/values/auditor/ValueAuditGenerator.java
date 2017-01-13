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
package gobblin.audit.values.auditor;

import java.io.Closeable;
import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;

import gobblin.audit.values.policy.column.ColumnProjectionPolicy;
import gobblin.audit.values.policy.column.DefaultColumnProjectionPolicyFactory;
import gobblin.audit.values.policy.row.DefaultRowSelectionPolicyFactory;
import gobblin.audit.values.policy.row.RowSelectionPolicy;
import gobblin.audit.values.sink.AuditSink;
import gobblin.audit.values.sink.DefaultAuditSinkFactory;


/**
 * The class that implements value based auditing. The class captures the values of certain
 * columns from the rows in the dataset using the {@link ColumnProjectionPolicy}.
 * This is done for every row or for a sample of the rows as defined by the {@link RowSelectionPolicy}.
 * The selected rows are then written to the {@link AuditSink}
 *
 * {@link ValueAuditGenerator#audit(GenericRecord)} is the method that audits an inputRecord.
 */
@AllArgsConstructor
@Getter
public class ValueAuditGenerator implements Closeable {

  public static final String COLUMN_PROJECTION_CONFIG_SCOPE = "columnProjection";
  public static final String ROW_SELECTION_CONFIG_SCOPE = "rowSelection";
  public static final String AUDIT_SINK_CONFIG_SCOPE = "auditSink";

  private final ColumnProjectionPolicy columnProjectionPolicy;
  private final RowSelectionPolicy rowSelectionPolicy;
  private final AuditSink auditSink;

  /**
   * Factory method to create a new {@link ValueAuditGenerator}
   * @param config job configs
   * @param runtimeAuditMetadata is used to pass the table specific runtime information like tablename, databaseName, snapshotName etc.
   *      See {@link ValueAuditRuntimeMetadata}
   * @return a new {@link ValueAuditGenerator}
   */
  public static ValueAuditGenerator create(Config config, ValueAuditRuntimeMetadata runtimeAuditMetadata) {
    ColumnProjectionPolicy columnProjectionPolicy = DefaultColumnProjectionPolicyFactory.getInstance().create(
        config.getConfig(COLUMN_PROJECTION_CONFIG_SCOPE),runtimeAuditMetadata.getTableMetadata());
    RowSelectionPolicy rowSelectionPolicy = DefaultRowSelectionPolicyFactory.getInstance().create(
        config.getConfig(ROW_SELECTION_CONFIG_SCOPE), runtimeAuditMetadata.getTableMetadata(), columnProjectionPolicy);
    AuditSink auditSink = DefaultAuditSinkFactory.getInstance().create(
        config.getConfig(AUDIT_SINK_CONFIG_SCOPE), runtimeAuditMetadata);

    return new ValueAuditGenerator(columnProjectionPolicy, rowSelectionPolicy, auditSink);
  }

  /**
   * Write an audit record for the <code>inputRecord</code> to the {@link AuditSink}.
   * An audit record is generated for every <code>inputRecord</code> that satisfies the {@link RowSelectionPolicy}.
   * An audit record is created by projecting  <code>inputRecord</code> using the {@link ColumnProjectionPolicy}
   *
   * @param inputRecord to be audited
   * @throws IOException if auditing failed for this record
   */
  public void audit(GenericRecord inputRecord) throws IOException {
    if (this.rowSelectionPolicy.shouldSelectRow(inputRecord)) {
      auditSink.write(columnProjectionPolicy.project(inputRecord));
    }
  }

  @Override
  public void close() throws IOException {
    this.auditSink.close();
  }
}
