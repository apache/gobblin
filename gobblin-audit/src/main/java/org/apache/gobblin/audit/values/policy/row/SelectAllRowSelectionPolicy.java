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
package gobblin.audit.values.policy.row;

import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import gobblin.audit.values.policy.column.ColumnProjectionPolicy;

/**
 * A {@link RowSelectionPolicy} that selects all rows for auditing
 */
@Alias(value = "SelectAll")
public class SelectAllRowSelectionPolicy extends AbstractRowSelectionPolicy {

  public SelectAllRowSelectionPolicy(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata, ColumnProjectionPolicy columnProjectionPolicy) {
    super(config, tableMetadata, columnProjectionPolicy);
  }

  /**
   * Return <code>true</code> always
   * {@inheritDoc}
   * @see gobblin.audit.values.policy.row.RowSelectionPolicy#shouldSelectRow(org.apache.avro.generic.GenericRecord)
   */
  @Override
  public boolean shouldSelectRow(GenericRecord genericRecord) {
    return true;
  }
}
