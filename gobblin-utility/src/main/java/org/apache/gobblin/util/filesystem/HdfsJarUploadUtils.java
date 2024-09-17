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

package org.apache.gobblin.util.filesystem;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;


/**
 * Utility class for uploading jar files to HDFS with retries to handle concurrency
 */
@Slf4j
public class HdfsJarUploadUtils {

  private static final long WAITING_TIME_ON_INCOMPLETE_UPLOAD_MILLIS = 3000;

  /**
   * Calculate the target filePath of the jar file to be copied on HDFS,
   * given the {@link FileStatus} of a jarFile and the path of directory that contains jar.
   * Snapshot dirs should not be shared, as different jobs may be using different versions of it.
   * @param fs
   * @param jarName
   * @param unsharedJarsDir
   * @param jarCacheDir
   * @return
   * @throws IOException
   */
  public static Path calculateDestJarFilePath(FileSystem fs, String jarName, Path unsharedJarsDir, Path jarCacheDir) throws IOException {
    Path uploadDir = jarName.contains("SNAPSHOT") ? unsharedJarsDir : jarCacheDir;
    Path destJarFile = new Path(fs.makeQualified(uploadDir), jarName);
    return destJarFile;
  }
  /**
   * Upload a jar file to HDFS with retries to handle already existing jars
   * @param fs
   * @param localJar
   * @param destJarFile
   * @param maxAttempts
   * @return
   * @throws IOException
   */
  public static boolean uploadJarToHdfs(FileSystem fs, FileStatus localJar, int maxAttempts, Path destJarFile) throws IOException {
    int retryCount = 0;
    while (!fs.exists(destJarFile) || fs.getFileStatus(destJarFile).getLen() != localJar.getLen()) {
      try {
        if (fs.exists(destJarFile) && fs.getFileStatus(destJarFile).getLen() != localJar.getLen()) {
          Thread.sleep(WAITING_TIME_ON_INCOMPLETE_UPLOAD_MILLIS);
          throw new IOException("Waiting for file to complete on uploading ... ");
        }
        boolean deleteSourceFile = false;
        boolean overwriteAnyExistingDestFile = false; // IOException will be thrown if does already exist
        fs.copyFromLocalFile(deleteSourceFile, overwriteAnyExistingDestFile, localJar.getPath(), destJarFile);
      } catch (IOException | InterruptedException e) {
        log.warn("Path:" + destJarFile + " is not copied successfully. Will require retry.");
        retryCount += 1;
        if (retryCount >= maxAttempts) {
          log.error("The jar file:" + destJarFile + "failed in being copied into hdfs", e);
          // If retry reaches upper limit, skip copying this file.
          return false;
        }
      }
    }
    return true;
  }
}
