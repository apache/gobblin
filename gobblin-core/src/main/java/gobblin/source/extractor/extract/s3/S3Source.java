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
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


/**
 * An implementation of an S3 source to get work units.
 * The source and destination buckets and paths are set in:
 * {@link ConfigurationKeys#S3_SOURCE_BUCKET},
 * {@link ConfigurationKeys#S3_SOURCE_PATH},
 * {@link ConfigurationKeys#S3_PUBLISHER_BUCKET}, and
 * {@link ConfigurationKeys#S3_PUBLISHER_PATH}.
 * <p/>
 * If you want your S3 paths to contain a date, the {@link S3Source} will
 * automatically check for you.
 * Relevant date manipulation values are:
 * {@link ConfigurationKeys#S3_DATE_PATTERN} (the pattern to match),
 * {@link ConfigurationKeys#S3_DATE_PLACEHOLDER} (the placeholder in the jobfile path), and
 * {@link ConfigurationKeys#S3_DATE_OFFSET} (the number of days offset (relative to the current date)
 * <p/>
 * If you do not wish, defaults are set for those values
 * and your paths will be unaffected.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Source extends AbstractSource<Class<String>, String> {

  public static final String TABLE_NAME = "default";
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "s3Source";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    AmazonS3 s3Client = new AmazonS3Client();
    String s3Bucket = state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET);
    String s3Path = state.getProp(ConfigurationKeys.S3_SOURCE_PATH);

    // Replace the date if needed (if none found, s3Path is unaffected
    s3Path = checkAndReplaceDate(state, s3Path);
    state.setProp(ConfigurationKeys.S3_SOURCE_PATH, s3Path);

    // Build the request
    ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Bucket).withPrefix(s3Path);

    // Fetch all the objects in the given bucket/path
    ObjectListing objectListing = s3Client.listObjects(listObjectRequest);
    List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

    // We have to do this if there are a large number of objects in the
    // given path
    while (objectListing.isTruncated()) {
      objectListing = s3Client.listNextBatchOfObjects(objectListing);
      objectSummaries.addAll(objectListing.getObjectSummaries());
    }

    // Generate a work unit for each object
    for (S3ObjectSummary summary : objectSummaries) {
      WorkUnit workUnit = getWorkUnitForS3Object(state, summary.getKey());
      if (workUnit != null) {
        workUnits.add(workUnit);
      }
    }

    return workUnits;
  }

  /**
   * If you want your S3 path to contain a date, you can replace it here
   * The placeholder is what the source looks for in the path, and replaces it
   * with the date (offset by S3_DATE_OFFSET), using the pattern to format it.
   * <p/>
   * If no date is matched in the path, nothing happens and it returns back
   * the string unchanged.
   *
   * @param state  The source state
   * @param s3Path The path to look in on S3
   * @return the s3Path with any date placeholders replaced with the specified date
   * pattern and offset.
   */
  private String checkAndReplaceDate(SourceState state, String s3Path) {
    String placeholder =
        state.getProp(ConfigurationKeys.S3_DATE_PLACEHOLDER, ConfigurationKeys.DEFAULT_S3_DATE_PLACEHOLDER);
    String datePattern = state.getProp(ConfigurationKeys.S3_DATE_PATTERN, ConfigurationKeys.DEFAULT_S3_DATE_PATTERN);
    // If set, 0 for today, -1 for yesterday, etc.
    int dateOffset = state.getPropAsInt(ConfigurationKeys.S3_DATE_OFFSET, ConfigurationKeys.DEFAULT_S3_DATE_OFFSET);

    SimpleDateFormat df = new SimpleDateFormat(datePattern);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, dateOffset);

    return s3Path.replace(placeholder, df.format(cal.getTime()));
  }

  /**
   * Generates a work unit for an S3 object. The object key is passed to
   * an extractor that will extract the object at the key joined with the source bucket.
   *
   * @param state           The source state
   * @param objectSourceKey The key of the object to pull from S3.
   * @return a work unit consisting of one S3 object.
   */
  private WorkUnit getWorkUnitForS3Object(SourceState state, String objectSourceKey) {
    SourceState partitionState = new SourceState();
    partitionState.addAll(state);

    String publisherPath = partitionState.getProp(ConfigurationKeys.S3_PUBLISHER_PATH);
    // Replace date placeholder if contained, otherwise this does nothing
    publisherPath = checkAndReplaceDate(partitionState, publisherPath);
    partitionState.setProp(ConfigurationKeys.S3_PUBLISHER_PATH, publisherPath);

    // Set the object key to be just the filename (with extension)
    // TODO alternatively, we could use a different naming schema
    partitionState.setProp("S3_OBJECT_KEY", FilenameUtils.getName(objectSourceKey));

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
  public Extractor<Class<String>, String> getExtractor(WorkUnitState state)
      throws IOException {
    return new S3StringExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
