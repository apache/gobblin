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
package gobblin.audit.values.policy.column;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;

/**
 * An {@link AbstractColumnProjectionPolicy} that projects all columns/fields of the <code>inputRecord</code>
 */
@Alias(value = "ProjectAll")
public class ProjectAllColumnProjectionPolicy extends AbstractColumnProjectionPolicy {

  public ProjectAllColumnProjectionPolicy(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata) {
    super(config, tableMetadata);
  }

  /**
   * Return the entire <code>inputRecord</code>. All fields are projected
   *
   * {@inheritDoc}
   * @see gobblin.audit.values.policy.column.ColumnProjectionPolicy#project(org.apache.avro.generic.GenericRecord)
   */
  @Override
  public GenericRecord project(GenericRecord inputRecord) {
    return inputRecord;
  }

  @Override
  public List<String> getKeyColumnsToProject() {
    return this.tableMetadata.getKeyFieldLocations();
  }

  @Override
  public List<String> getDeltaColumnsToProject() {
    return this.tableMetadata.getDeltaFieldLocations();
  }
}
