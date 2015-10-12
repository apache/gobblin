/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.util.RecordCountProvider;


/**
 * Class used with {@link MRCompactorAvroKeyDedupJobRunner} to rename files as they
 * are being committed. In addition to moving files from their working directory to
 * the commit output directory, the files are named to include a timestamp and a
 * count of how many records the file contains, in the format
 * {recordCount}.{timestamp}.avro.
 */
public class AvroKeyCompactorOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyCompactorOutputCommitter.class);

  private static final String MR_OUTPUT_FILE_PREFIX = "part-r-";
  private static final String M_OUTPUT_FILE_PREFIX = "part-m-";

  private static final Random RANDOM = new Random();

  public AvroKeyCompactorOutputCommitter(Path output, TaskAttemptContext context) throws IOException {
    super(output, context);
  }

  /**
   * Commits the task, moving files to their final committed location by delegating to
   * {@link FileOutputCommitter} to perform the actual moving. First, renames the
   * files to include the count of records contained within the file and a timestamp,
   * in the form {recordCount}.{timestamp}.avro. Then, the files are moved to their
   * committed location.
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    Path workPath = getWorkPath();
    FileSystem fs = workPath.getFileSystem(context.getConfiguration());

    if (fs.exists(workPath)) {
      long recordCount = getRecordCountFromCounter(context, AvroKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);
      String fileNamePrefix;
      if (recordCount == 0) {

        // recordCount == 0 indicates that it is a map-only, non-dedup job, and thus record count should
        // be obtained from mapper counter.
        fileNamePrefix = M_OUTPUT_FILE_PREFIX;
        recordCount = getRecordCountFromCounter(context, AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);
      } else {
        fileNamePrefix = MR_OUTPUT_FILE_PREFIX;
      }
      String fileName =
          new FilenameRecordCountProvider().constructFileName(fileNamePrefix, recordCount);

      for (FileStatus status : fs.listStatus(workPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return FilenameUtils.isExtension(path.getName(), "avro");
        }
      })) {
        Path newPath = new Path(status.getPath().getParent(), fileName);
        LOG.info(String.format("Renaming %s to %s", status.getPath(), newPath));
        fs.rename(status.getPath(), newPath);
      }
    }

    super.commitTask(context);
  }

  private long getRecordCountFromCounter(TaskAttemptContext context, Enum<?> counterName) {
    try {
      //In Hadoop 2, TaskAttemptContext.getCounter() is available
      Method getCounterMethod = context.getClass().getMethod("getCounter", Enum.class);
      return ((Counter) getCounterMethod.invoke(context, counterName)).getValue();
    } catch (NoSuchMethodException e) {
      //In Hadoop 1, TaskAttemptContext.getCounter() is not available
      //Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
      org.apache.hadoop.mapred.TaskAttemptContext mapredContext = (org.apache.hadoop.mapred.TaskAttemptContext) context;
      return ((StatusReporter) mapredContext.getProgressible()).getCounter(counterName).getValue();
    } catch (Exception e) {
      throw new RuntimeException("Error reading record count counter", e);
    }
  }

  /**
   * Implementation of {@link RecordCountProvider}, which provides record count from file path.
   * The file name should follow the pattern: {Prefix}{RecordCount}.{SystemCurrentTimeInMills}.{RandomInteger}{SUFFIX}.
   * The prefix should be either {@link AvroKeyCompactorOutputCommitter#M_OUTPUT_FILE_PREFIX} or {@link AvroKeyCompactorOutputCommitter#MR_OUTPUT_FILE_PREFIX}.
   * For example, given a file path: "/a/b/c/part-m-123.1444437036.12345.avro", the record count will be 123.
   */
  public static class FilenameRecordCountProvider implements RecordCountProvider {
    private static final String SEPARATOR = ".";
    private static final String SUFFIX = ".avro";

    /**
     * Construct the file name as {filenamePrefix}{recordCount}.{SystemCurrentTimeInMills}.{RandomInteger}{SUFFIX}.
     */
    public String constructFileName(String filenamePrefix, long recordCount) {
      Preconditions.checkArgument(
          filenamePrefix.equals(M_OUTPUT_FILE_PREFIX) || filenamePrefix.equals(MR_OUTPUT_FILE_PREFIX), String.format(
              "%s is not a supported prefix, which should be %s, or %s.", filenamePrefix, M_OUTPUT_FILE_PREFIX,
              MR_OUTPUT_FILE_PREFIX));
      StringBuilder sb = new StringBuilder();
      sb.append(filenamePrefix);
      sb.append(Long.toString(recordCount));
      sb.append(SEPARATOR);
      sb.append(Long.toString(System.currentTimeMillis()));
      sb.append(SEPARATOR);
      sb.append(Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE)));
      sb.append(SUFFIX);
      return sb.toString();
    }

    /**
     * Get the record count through filename.
     */
    @Override
    public long getRecordCount(Path filepath) {
      String filename = filepath.getName();
      Preconditions.checkArgument(
          filename.startsWith(M_OUTPUT_FILE_PREFIX) || filename.startsWith(MR_OUTPUT_FILE_PREFIX), String.format(
              "%s is not a supported filename, which should start with %s, or %s.", filename, M_OUTPUT_FILE_PREFIX,
              MR_OUTPUT_FILE_PREFIX));
      String prefixWithCounts = filename.split(Pattern.quote(SEPARATOR))[0];
      if (filename.startsWith(M_OUTPUT_FILE_PREFIX)) {
        return Long.parseLong(prefixWithCounts.substring(M_OUTPUT_FILE_PREFIX.length()));
      } else {
        return Long.parseLong(prefixWithCounts.substring(MR_OUTPUT_FILE_PREFIX.length()));
      }
    }
  }
}
