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

import com.google.common.annotations.VisibleForTesting;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;


/**
 * {@link CommitStep} to perform Iceberg registration.  It is critically important to use the same source-side {@link TableMetadata} observed while
 * listing the source table and the dest-side {@link TableMetadata} observed just prior to that listing of the source table.  Either table may have
 * changed between first calculating the source-to-dest difference and now performing the commit on the destination (herein).  Accordingly, use of
 * now-current metadata could thwart consistency.  Only metadata preserved from the time of the difference calc guarantees correctness.
 *
 *   - if the source table has since changed, we nonetheless use the metadata originally observed, since now-current metadata wouldn't match the
 *     files just copied to dest
 *   - if the dest table has since changed, we reject the commit altogether to force the diff calc to re-start again (in a subsequent execution)
 */
@Slf4j
public class IcebergRegisterStep implements CommitStep {

  // store as string for serializability... TODO: explore whether truly necessary (or we could just as well store as `TableIdentifier`)
  private final String srcTableIdStr; // used merely for naming within trace logging
  private final String destTableIdStr;
  private final TableMetadata readTimeSrcTableMetadata;
  private final TableMetadata justPriorDestTableMetadata;
  private final Properties properties;

  public IcebergRegisterStep(TableIdentifier srcTableId, TableIdentifier destTableId,
      TableMetadata readTimeSrcTableMetadata, TableMetadata justPriorDestTableMetadata,
      Properties properties) {
    this.srcTableIdStr = srcTableId.toString();
    this.destTableIdStr = destTableId.toString();
    this.readTimeSrcTableMetadata = readTimeSrcTableMetadata;
    this.justPriorDestTableMetadata = justPriorDestTableMetadata;
    this.properties = properties;
  }

  @Override
  public boolean isCompleted() throws IOException {
    return false;
  }

  @Override
  public void execute() throws IOException {
    IcebergTable destIcebergTable = createDestinationCatalog().openTable(TableIdentifier.parse(destTableIdStr));
    try {
      TableMetadata currentDestMetadata = destIcebergTable.accessTableMetadata(); // probe... (first access could throw)
      // CRITICAL: verify current dest-side metadata remains the same as observed just prior to first loading source catalog table metadata
      boolean isJustPriorDestMetadataStillCurrent = currentDestMetadata.uuid().equals(justPriorDestTableMetadata.uuid())
          && currentDestMetadata.metadataFileLocation().equals(justPriorDestTableMetadata.metadataFileLocation());
      String determinationMsg = String.format("(just prior) TableMetadata: {} - {} {}= (current) TableMetadata: {} - {}",
          justPriorDestTableMetadata.uuid(), justPriorDestTableMetadata.metadataFileLocation(),
          isJustPriorDestMetadataStillCurrent ? "=" : "!",
          currentDestMetadata.uuid(), currentDestMetadata.metadataFileLocation());
      log.info("~{}~ [destination] {}", destTableIdStr, determinationMsg);

      // NOTE: we originally expected the dest-side catalog to enforce this match, but the client-side `BaseMetastoreTableOperations.commit`
      // uses `==`, rather than `.equals` (value-cmp), and that invariably leads to:
      //   org.apache.iceberg.exceptions.CommitFailedException: Cannot commit: stale table metadata
      if (!isJustPriorDestMetadataStillCurrent) {
        throw new IOException("error: likely concurrent writing to destination: " + determinationMsg);
      }
      destIcebergTable.registerIcebergTable(readTimeSrcTableMetadata, currentDestMetadata);
    } catch (IcebergTable.TableNotFoundException tnfe) {
      String msg = "Destination table (with TableMetadata) does not exist: " + tnfe.getMessage();
      log.error(msg);
      throw new IOException(msg, tnfe);
    }
  }

  @Override
  public String toString() {
    return String.format("Registering Iceberg Table: {%s} (dest); (src: {%s})", this.destTableIdStr, this.srcTableIdStr);
  }

  /** Purely because the static `IcebergDatasetFinder.createIcebergCatalog` proved challenging to mock, even w/ `Mockito::mockStatic` */
  @VisibleForTesting
  protected IcebergCatalog createDestinationCatalog() throws IOException {
    return IcebergDatasetFinder.createIcebergCatalog(this.properties, IcebergDatasetFinder.CatalogLocation.DESTINATION);
  }
}
