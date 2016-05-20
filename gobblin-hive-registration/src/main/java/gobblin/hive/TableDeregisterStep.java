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

package gobblin.hive;

import lombok.AllArgsConstructor;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Optional;

import gobblin.commit.CommitStep;


/**
 * {@link CommitStep} to deregister a Hive table.
 */
@AllArgsConstructor
public class TableDeregisterStep implements CommitStep {

  private Table table;
  private final Optional<String> metastoreURI;
  private final HiveRegProps props;

  @Override
  public boolean isCompleted() throws IOException {
    return false;
  }

  @Override
  public void execute() throws IOException {
    try (HiveRegister hiveRegister = HiveRegister.get(this.props, this.metastoreURI)) {
      hiveRegister.dropTableIfExists(this.table.getDbName(), this.table.getTableName());
    }
  }

  @Override
  public String toString() {
    return String.format("Deregister table %s.%s on Hive metastore %s.", this.table.getDbName(),
        this.table.getTableName(), 
        this.metastoreURI.isPresent() ? this.metastoreURI.get() : "default");
  }
}
