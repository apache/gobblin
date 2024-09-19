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

import org.apache.gobblin.commit.CommitStep;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.SerializationUtil;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class IcebergReplacePartitionsStep implements CommitStep {
  private final String destTableIdStr;
  private final Properties properties;
  private final byte[] serializedDataFiles;

  public IcebergReplacePartitionsStep(String destTableIdStr, byte[] serializedDataFiles, Properties properties) {
    this.destTableIdStr = destTableIdStr;
    this.serializedDataFiles = serializedDataFiles;
    this.properties = properties;
  }
  @Override
  public boolean isCompleted() {
    return false;
  }

  @Override
  public void execute() throws IOException {
    IcebergTable destTable = createDestinationCatalog().openTable(TableIdentifier.parse(destTableIdStr));
    try {
      log.info("Replacing partitions for table " + destTableIdStr);
      destTable.replacePartitions(SerializationUtil.deserializeFromBytes(this.serializedDataFiles));
      log.info("Replaced partitions for table " + destTableIdStr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected IcebergCatalog createDestinationCatalog() throws IOException {
    return IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.DESTINATION);
  }

}
