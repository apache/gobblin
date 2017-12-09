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

package org.apache.gobblin.util.recordcount;

import java.util.Random;
import java.util.regex.Pattern;

import org.apache.gobblin.util.RecordCountProvider;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;


/**
 * Implementation of {@link RecordCountProvider}, which provides record count from file path.
 * The file name should follow the pattern: {Prefix}{RecordCount}.{SystemCurrentTimeInMills}.{RandomInteger}{SUFFIX}.
 * The prefix should be either {@link #M_OUTPUT_FILE_PREFIX} or {@link #MR_OUTPUT_FILE_PREFIX}.
 * For example, given a file path: "/a/b/c/part-m-123.1444437036.12345.avro", the record count will be 123.
 */
public class CompactionRecordCountProvider extends RecordCountProvider {

  public static final String MR_OUTPUT_FILE_PREFIX = "part-r-";
  public static final String M_OUTPUT_FILE_PREFIX = "part-m-";

  private static final String SEPARATOR = ".";
  private static final String SUFFIX = ".avro";
  private static final Random RANDOM = new Random();

  /**
   * Construct the file name as {filenamePrefix}{recordCount}.{SystemCurrentTimeInMills}.{RandomInteger}{SUFFIX}.
   */
  public static String constructFileName(String filenamePrefix, long recordCount) {
    return constructFileName(filenamePrefix, SUFFIX, recordCount);
  }

  /**
   * Construct the file name as {filenamePrefix}{recordCount}.{SystemCurrentTimeInMills}.{RandomInteger}{extension}.
   */
  public static String constructFileName(String filenamePrefix, String extension, long recordCount) {
    Preconditions.checkArgument(
        filenamePrefix.equals(M_OUTPUT_FILE_PREFIX) || filenamePrefix.equals(MR_OUTPUT_FILE_PREFIX),
        String.format("%s is not a supported prefix, which should be %s, or %s.", filenamePrefix, M_OUTPUT_FILE_PREFIX,
            MR_OUTPUT_FILE_PREFIX));
    StringBuilder sb = new StringBuilder();
    sb.append(filenamePrefix);
    sb.append(Long.toString(recordCount));
    sb.append(SEPARATOR);
    sb.append(Long.toString(System.currentTimeMillis()));
    sb.append(SEPARATOR);
    sb.append(Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE)));
    sb.append(extension);
    return sb.toString();
  }


  /**
   * Get the record count through filename.
   */
  @Override
  public long getRecordCount(Path filepath) {
    String filename = filepath.getName();
    Preconditions.checkArgument(filename.startsWith(M_OUTPUT_FILE_PREFIX) || filename.startsWith(MR_OUTPUT_FILE_PREFIX),
        String.format("%s is not a supported filename, which should start with %s, or %s.", filename,
            M_OUTPUT_FILE_PREFIX, MR_OUTPUT_FILE_PREFIX));
    String prefixWithCounts = filename.split(Pattern.quote(SEPARATOR))[0];
    if (filename.startsWith(M_OUTPUT_FILE_PREFIX)) {
      return Long.parseLong(prefixWithCounts.substring(M_OUTPUT_FILE_PREFIX.length()));
    }
    return Long.parseLong(prefixWithCounts.substring(MR_OUTPUT_FILE_PREFIX.length()));
  }

  /**
   * This method currently supports converting the given {@link Path} from {@link IngestionRecordCountProvider}.
   * The converted {@link Path} will start with {@link #M_OUTPUT_FILE_PREFIX}.
   */
  public Path convertPath(Path path, String extension, RecordCountProvider src) {
    if (this.getClass().equals(src.getClass())) {
      return path;
    } else if (src.getClass().equals(IngestionRecordCountProvider.class)) {
      String newFileName = constructFileName(M_OUTPUT_FILE_PREFIX, extension, src.getRecordCount(path));
      return new Path(path.getParent(), newFileName);
    } else {
      throw getNotImplementedException(src);
    }
  }
}
