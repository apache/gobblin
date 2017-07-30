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
