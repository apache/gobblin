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

package org.apache.gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.util.recordcount.CompactionRecordCountProvider;


/**
 * Class used with {@link MRCompactorAvroKeyDedupJobRunner} to rename files as they
 * are being committed. In addition to moving files from their working directory to
 * the commit output directory, the files are named to include a timestamp and a
 * count of how many records the file contains, in the format
 * {recordCount}.{timestamp}.avro.
 */
public class AvroKeyCompactorOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyCompactorOutputCommitter.class);

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
        fileNamePrefix = CompactionRecordCountProvider.M_OUTPUT_FILE_PREFIX;
        recordCount = getRecordCountFromCounter(context, AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);
      } else {
        fileNamePrefix = CompactionRecordCountProvider.MR_OUTPUT_FILE_PREFIX;
      }
      String fileName = CompactionRecordCountProvider.constructFileName(fileNamePrefix, recordCount);

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

  private static long getRecordCountFromCounter(TaskAttemptContext context, Enum<?> counterName) {
    try {
      Method getCounterMethod = context.getClass().getMethod("getCounter", Enum.class);
      return ((Counter) getCounterMethod.invoke(context, counterName)).getValue();
    } catch (Exception e) {
      throw new RuntimeException("Error reading record count counter", e);
    }
  }
}
