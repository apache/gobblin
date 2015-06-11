/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.source.extractor.extract.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of an s3 source to get work units
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Source extends AbstractSource<Class<String>, ArrayList<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(S3Source.class);

  public static final String TABLE_NAME = "table.name";
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "KAFKA";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    AmazonS3 s3Client = new AmazonS3Client();

    String s3Bucket = state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET);
    String s3Path = state.getProp(ConfigurationKeys.S3_SOURCE_PATH);

    ListObjectsRequest listObjectRequest = new ListObjectsRequest()
            .withBucketName(s3Bucket)
            .withPrefix(s3Path);

    // Fetch all the objects in the given bucket/path
    ObjectListing objectListing = s3Client.listObjects(listObjectRequest);
    List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

    while(objectListing.isTruncated()) {
      objectListing = s3Client.listNextBatchOfObjects(objectListing);
      objectSummaries.addAll(objectListing.getObjectSummaries());
    }

    // Generate a work unit for each object
    for(S3ObjectSummary summary : objectSummaries) {
      WorkUnit workUnit = getWorkUnitForS3Object(state, summary.getKey());
      if (workUnit != null) {
        workUnits.add(workUnit);
      }
    }

    return workUnits;
  }

  /**
   * Generates a work unit for an S3 object. The object key is passed to
   * an extractor that will extract the object at the key joined with the source bucket.
   *
   * @param state - The source state
   * @param objectKey - The key of the object to pull from S3.
   * @return a work unit consisting of one S3 object.
   */
  private WorkUnit getWorkUnitForS3Object(SourceState state, String objectKey) {
    SourceState partitionState = new SourceState();
    partitionState.addAll(state);
    partitionState.setProp("OBJECT_KEY", objectKey);
    Extract extract = partitionState.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, TABLE_NAME);
    return partitionState.createWorkUnit(extract);
  }

  /**
   * Get an {@link Extractor} based on a given {@link WorkUnitState}.
   * <p>
   * The {@link Extractor} returned can use {@link WorkUnitState} to store arbitrary key-value pairs
   * that will be persisted to the state store and loaded in the next scheduled job run.
   * </p>
   *
   * @param state a {@link WorkUnitState} carrying properties needed by the returned {@link Extractor}
   * @return an {@link Extractor} used to extract schema and data records from the data source
   * @throws IOException if it fails to create an {@link Extractor}
   */
  @Override
  public Extractor<Class<String>, ArrayList<String>> getExtractor(WorkUnitState state) throws IOException {
    return new S3CSVExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
