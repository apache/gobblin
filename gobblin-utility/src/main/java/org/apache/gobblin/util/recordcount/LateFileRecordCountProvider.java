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

import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.util.RecordCountProvider;
import lombok.AllArgsConstructor;


@AllArgsConstructor
public class LateFileRecordCountProvider extends RecordCountProvider {
  private static final String SEPARATOR = ".";
  private static final String LATE_COMPONENT = ".late";
  private static final String EMPTY_STRING = "";

  private RecordCountProvider recordCountProviderWithoutSuffix;

  /**
   * Construct filename for a late file. If the file does not exists in the output dir, retain the original name.
   * Otherwise, append a LATE_COMPONENT{RandomInteger} to the original file name.
   * For example, if file "part1.123.avro" exists in dir "/a/b/", the returned path will be "/a/b/part1.123.late12345.avro".
   */
  public Path constructLateFilePath(String originalFilename, FileSystem fs, Path outputDir) throws IOException {
    if (!fs.exists(new Path(outputDir, originalFilename))) {
      return new Path(outputDir, originalFilename);
    }
    return constructLateFilePath(FilenameUtils.getBaseName(originalFilename) + LATE_COMPONENT
        + new Random().nextInt(Integer.MAX_VALUE) + SEPARATOR + FilenameUtils.getExtension(originalFilename), fs,
        outputDir);
  }

  /**
   * Remove the late components in the path added by {@link LateFileRecordCountProvider}.
   */
  public static Path restoreFilePath(Path path) {
    return new Path(path.getName().replaceAll(Pattern.quote(LATE_COMPONENT) + "[\\d]*", EMPTY_STRING));
  }

  /**
   * Get record count from a given filename (possibly having LATE_COMPONENT{RandomInteger}), using the original {@link FileNameWithRecordCountFormat}.
   */
  @Override
  public long getRecordCount(Path path) {
    return this.recordCountProviderWithoutSuffix.getRecordCount(restoreFilePath(path));
  }
}
