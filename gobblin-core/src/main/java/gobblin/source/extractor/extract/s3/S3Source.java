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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.FileBasedSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of an S3 source to get work units.
 * The source buckets and paths are set in
 * {@link ConfigurationKeys#S3_SOURCE_BUCKET} and {@link ConfigurationKeys#S3_SOURCE_PATH}
 * <p/>
 * If you want your S3 paths to contain a date, the {@link S3Source} will
 * automatically check for you.
 * Relevant date manipulation values are:
 * {@link ConfigurationKeys#S3_SOURCE_DATE_PATTERN} (the pattern to match),
 * {@link ConfigurationKeys#S3_SOURCE_DATE_PLACEHOLDER} (the placeholder in the jobfile path), and
 * {@link ConfigurationKeys#S3_SOURCE_DATE_OFFSET} (the number of days offset (relative to the current date)
 * <p/>
 * If you do not wish, defaults are set for those values
 * and your paths will be unaffected.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Source<S, D> extends FileBasedSource<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(S3Source.class);

  /**
   * Get an {@link S3Extractor} based on a given {@link WorkUnitState}.
   * <p>
   * The {@link S3Extractor} returned can use {@link WorkUnitState} to store arbitrary key-value pairs
   * that will be persisted to the state store and loaded in the next scheduled job run.
   * </p>
   *
   * @param state a {@link WorkUnitState} carrying properties needed by the returned {@link S3Extractor}
   * @return an {@link S3Extractor} used to extract schema and data records from the data source
   * @throws IOException if it fails to create an {@link S3Extractor}
   */
  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state)
      throws IOException {
    return new S3Extractor<S, D>(state);
  }

  @Override
  public void initFileSystemHelper(State state)
      throws FileBasedHelperException {
    this.fsHelper = new S3FsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = new ArrayList<String>();
    S3FsHelper s3FsHelper = (S3FsHelper) this.fsHelper;
    List<String> paths = s3FsHelper.getS3Paths();

    for(String path : paths) {
      try {
        LOG.info("Running ls command with input " + path);
        results.addAll(this.fsHelper.ls(path));
      } catch (FileBasedHelperException e) {
        LOG.error("ls command unsuccessful - " + e.getMessage() + " will not pull any files", e);
      }
    }

    if(results.size() == 0) {
      LOG.error("Either there were no new files or no files found in the specified path(s).");
    }

    return results;
  }
}
