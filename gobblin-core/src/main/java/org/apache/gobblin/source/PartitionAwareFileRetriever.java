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
package gobblin.source;

import java.io.IOException;
import java.util.List;

import gobblin.configuration.SourceState;


/**
 * Interface for an object that, given a sourcePath and a low watermark, can return a set of files
 * to process that are newer than the low watermark.
 */
public interface PartitionAwareFileRetriever {
  /**
   * Initialize the retriever with configuration parameters
   */
  void init(SourceState state);

  /*
   * Retrieve a watermark (in milliseconds since epoch) from a String representation.
   * Users can set a low watermark in the `date.partitioned.source.min.watermark.value` configuration setting,
   * and this function translates that into an internal watermark.
   *
   * Actual format is up to the developer but generally this is some sort of DateTimeFormatter (eg translate 2000-01-01
   * to the right value).
   */
  long getWatermarkFromString(String watermark);

  /*
   * Amount of milliseconds to increment the watermark after a batch of files has been processed. For example,
   * a daily-based partitioner may want to increment the watermark by 24 hours to see the next batch of files
   * in a dataset.
   */
  long getWatermarkIncrementMs();

  /**
   * Return a list of files to process that have a watermark later than minWatermark. Generally, a FileRetriever should
   * find each valid partition after minWatermark, sorted by ascending time.
   *
   * For each partition:
   *   1. Add all files in the partition
   *   2. If the # of files in the return list is now greater than maxFilesToReturn, return immediately
   *   3. Else continue to next partition until there are none left
   *
   * maxFilesToReturn is a soft cap - all files in a partition should be returned by getFilesToProcess().
   */
  List<FileInfo> getFilesToProcess(long minWatermark, int maxFilesToReturn) throws IOException;

  public static class FileInfo implements Comparable<FileInfo> {
    private final String filePath;
    private final long fileSize;
    private final long watermarkMsSinceEpoch;

    public FileInfo(String filePath, long fileSize, long watermarkMsSinceEpoch) {
      this.fileSize = fileSize;
      this.filePath = filePath;
      this.watermarkMsSinceEpoch = watermarkMsSinceEpoch;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getWatermarkMsSinceEpoch() {
      return watermarkMsSinceEpoch;
    }

    public long getFileSize() {
      return fileSize;
    }

    @Override
    public String toString() {
      return "FileInfo{" + "filePath='" + filePath + '\'' + ", watermarkMsSinceEpoch=" + watermarkMsSinceEpoch + '}';
    }

    @Override
    public int compareTo(FileInfo o) {
      if (watermarkMsSinceEpoch < o.watermarkMsSinceEpoch) {
        return -1;
      } else if (watermarkMsSinceEpoch > o.watermarkMsSinceEpoch) {
        return 1;
      } else {
        return filePath.compareTo(o.filePath);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FileInfo fileInfo = (FileInfo) o;

      if (watermarkMsSinceEpoch != fileInfo.watermarkMsSinceEpoch) {
        return false;
      }
      return filePath != null ? filePath.equals(fileInfo.filePath) : fileInfo.filePath == null;
    }

    @Override
    public int hashCode() {
      int result = filePath != null ? filePath.hashCode() : 0;
      result = 31 * result + (int) (watermarkMsSinceEpoch ^ (watermarkMsSinceEpoch >>> 32));
      return result;
    }
  }
}
