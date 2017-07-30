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
package gobblin.audit.values.policy.column;

import java.util.List;

import org.apache.avro.Schema;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;


/**
 * A base {@link ColumnProjectionPolicy} that reads <code>config</code> to initialize the key and delta columns to project for a table.
 * <ul>
 * <li> Key and delta fields/column locations to project should be provided by concrete subclasses by implementing {@link #getDeltaColumnsToProject()}
 * and {@link #getKeyColumnsToProject()}
 * <li> The protected member <code>tableMetadata</code> contains the {@link Schema}, tableName and databaseName to derive the projection columns
 * </ul>
 */
public abstract class AbstractColumnProjectionPolicy implements ColumnProjectionPolicy {

  protected final ValueAuditRuntimeMetadata.TableMetadata tableMetadata;
  public AbstractColumnProjectionPolicy(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  /**
   * Combine both key columns and delta columns to project
   * {@inheritDoc}
   * @see gobblin.audit.values.policy.column.ColumnProjectionPolicy#getAllColumnsToProject()
   */
  public List<String> getAllColumnsToProject() {
    return ImmutableList.<String> builder().addAll(getKeyColumnsToProject()).addAll(getDeltaColumnsToProject()).build();
  }
}
