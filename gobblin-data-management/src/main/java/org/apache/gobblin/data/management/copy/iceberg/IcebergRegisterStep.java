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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.Properties;

import org.apache.iceberg.TableMetadata;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;

/**
 * {@link CommitStep} to perform Iceberg registration.
 */
@Slf4j
@AllArgsConstructor
public class IcebergRegisterStep implements CommitStep {

  private final String dbName;
  private final String tblName;
  private final Properties properties;

  @Override
  public boolean isCompleted() throws IOException {
    return false;
  }

  @Override
  public void execute() throws IOException {
    IcebergTable srcIcebergTable = IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.SOURCE)
        .openTable(this.dbName, this.tblName);
    IcebergTable destIcebergTable = IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.DESTINATION)
        .openTable(this.dbName, this.tblName);
    TableMetadata destinationMetadata = null;
    try {
      destinationMetadata = destIcebergTable.accessTableMetadata();
    } catch (IcebergTable.TableNotFoundException tnfe) {
      log.warn("Destination TableMetadata doesn't exist because: " , tnfe);
    }
    destIcebergTable.registerIcebergTable(srcIcebergTable.accessTableMetadata(), destinationMetadata);
  }
  @Override
  public String toString() {
    return String.format("Registering Iceberg Table: {%s}.{%s} ", this.dbName, this.tblName);
  }
}
