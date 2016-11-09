/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.audit.values.policy.row;

import com.typesafe.config.Config;

import gobblin.audit.values.auditor.ValueAuditGenerator;
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import gobblin.audit.values.policy.column.ColumnProjectionPolicy;


/**
 * An abstract {@link RowSelectionPolicy} that contains references to {@link ValueAuditRuntimeMetadata.TableMetadata} and
 * {@link ColumnProjectionPolicy} used by the {@link ValueAuditGenerator}.
 * Concrete classes need to implement {@link RowSelectionPolicy#shouldSelectRow(org.apache.avro.generic.GenericRecord)}
 */
public abstract class AbstractRowSelectionPolicy implements RowSelectionPolicy {
  protected final ValueAuditRuntimeMetadata.TableMetadata tableMetadata;
  protected final ColumnProjectionPolicy columnProjectionPolicy;

  public AbstractRowSelectionPolicy(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata,
      ColumnProjectionPolicy columnProjectionPolicy) {
    this.tableMetadata = tableMetadata;
    this.columnProjectionPolicy = columnProjectionPolicy;
  }
}
