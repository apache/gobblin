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

package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.S3Utils;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;


/**
 * Publishes files to S3. The destination bucket and path can be set
 * in {@link ConfigurationKeys#S3_PUBLISHER_BUCKET} and {@link ConfigurationKeys#S3_PUBLISHER_PATH}.
 * <p/>
 * You can set the destination filename of S3 objects. The filename is set using
 * {@link ConfigurationKeys#S3_PUBLISHER_FILENAME_FORMAT}, and the file extension is set by
 * {@link ConfigurationKeys#WRITER_OUTPUT_FORMAT_KEY}. View the filename format docs for a list
 * of valid placeholders.
 * <p/>
 * This will take all files fed into the job and combine them into one if {@link
 * ConfigurationKeys#S3_PUBLISHER_APPEND} is set to true. Otherwise, it will upload 1:1 input files to output files.
 * <b>Be careful with your filenames, as you might have naming conflicts if your filename format is
 * not sufficiently unique.</b>
 *
 * @author ahollenbach@nerdwallet.com
 */
public class SimpleS3Publisher extends BaseS3Publisher {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleS3Publisher.class);

  /**
   * Creates a new SimpleS3Publisher
   *
   * @param state The state
   */
  public SimpleS3Publisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states)
      throws IOException {
    String s3Bucket = this.getState().getProp(ConfigurationKeys.S3_PUBLISHER_BUCKET);
    String s3Path = this.getState().getProp(ConfigurationKeys.S3_PUBLISHER_PATH);
    s3Path = S3Utils.checkAndReplaceDates(this.getState(), s3Path);

    boolean append = this.getState().getPropAsBoolean(ConfigurationKeys.S3_PUBLISHER_APPEND,
        ConfigurationKeys.DEFAULT_S3_PUBLISHER_APPEND);

    if (append) {
      publishDataAsGroup(states, s3Bucket, s3Path);
    } else {
      publishDataSeparately(states, s3Bucket, s3Path);
    }
  }

  /**
   * Takes all of the files and writes them appended as one big file
   *
   * @param states The work unit states
   * @param s3Bucket The S3 bucket to publish to
   * @param s3Path The S3 path to publish to (raw)
   * @throws IOException
   */
  private void publishDataAsGroup(Collection<? extends WorkUnitState> states, String s3Bucket, String s3Path)
      throws IOException {
    ArrayList<String> writerFileNames = new ArrayList<String>();

    String s3Filename = generateObjectName(this.getState(), 0);
    s3Path = S3Utils.checkAndReplaceDates(this.getState(), s3Path);

    for (WorkUnitState state : states) {
      for (int i = 0; i < this.numBranches; i++) {
        String writerFileName =
            state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i));
        writerFileNames.add(writerFileName);
      }
    }
    this.sendS3Data(0, new BucketAndKey(s3Bucket, s3Path + s3Filename), writerFileNames);

    for (WorkUnitState state : states) {
      state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  /**
   * Takes all of the files and writes each separately to S3
   *
   * @param states The work unit states
   * @param s3Bucket The S3 bucket to publish to
   * @param s3Path The S3 path to publish to (raw)
   * @throws IOException
   */
  private void publishDataSeparately(Collection<? extends WorkUnitState> states, String s3Bucket, String s3Path)
      throws IOException {
    int counter = 0;  // for filename counting
    for (WorkUnitState state : states) {
      for (int i = 0; i < this.numBranches; i++) {
        ArrayList<String> writerFileNames = new ArrayList<String>();
        String writerFileName =
            state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i));
        writerFileNames.add(writerFileName);

        s3Path = S3Utils.checkAndReplaceDates(this.getState(), s3Path);
        String s3Filename = generateObjectName(state, counter++);

        this.sendS3Data(i, new BucketAndKey(s3Bucket, s3Path + s3Filename), writerFileNames);
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
    }
  }

  /**
   * Generates the object name (filename) for placing in S3.
   * <p/>
   * If {@link ConfigurationKeys#S3_PUBLISHER_FILENAME_FORMAT} is not specified, this will try to fetch an object
   * called "S3_SOURCE_OBJECT_KEY" and use that as a fallback. If this does not exist, this will fallback to use
   * just the value of counter as the filename.
   *
   * @param state The work unit state
   * @param counter The filename counter to append to the filename. This is used only if
   *                {@link ConfigurationKeys#S3_PUBLISHER_FILENAME_FORMAT} is set and specifies its use.
   * @return the filename of the object to place (no extension - this is added by the writer)
   */
  protected String generateObjectName(State state, int counter) {
    // Try to use the given filename format.
    String filenameFormat = state.getProp(ConfigurationKeys.S3_PUBLISHER_FILENAME_FORMAT);
    if (filenameFormat != null) {
      // If we use a filename format, replace out any date placeholders ({date},{now})
      filenameFormat = S3Utils.checkAndReplaceDates(state, filenameFormat);

      // Replace the counter placeholder, if any
      filenameFormat = filenameFormat.replace("{counter}", Integer.toString(counter));

      // Replace any slashes with nothing
      filenameFormat = filenameFormat.replace("/", "");

      return filenameFormat;
    }

    // If the source stored the source key, just use that as a fallback
    String sourceKey = state.getProp("S3_SOURCE_OBJECT_KEY");
    if(sourceKey != null) {
      return FilenameUtils.getBaseName(sourceKey);
    }

    // Otherwise, just return the counter as a filename
    return Integer.toString(counter);
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states)
      throws IOException {

  }
}
