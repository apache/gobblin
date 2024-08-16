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

  private static final long WAITING_TIME_ON_IMCOMPLETE_UPLOAD = 3000;

  /**
   * Calculate the target filePath of the jar file to be copied on HDFS,
   * given the {@link FileStatus} of a jarFile and the path of directory that contains jar.
   * Snapshot dirs should not be shared, as different jobs may be using different versions of it.
   * @param fs
   * @param localJar
   * @param unsharedJarsDir
   * @param jarCacheDir
   * @return
   * @throws IOException
   */
  public static Path calculateDestJarFile(FileSystem fs, FileStatus localJar, Path unsharedJarsDir, Path jarCacheDir) throws IOException {
    Path uploadDir = localJar.getPath().getName().contains("SNAPSHOT") ? unsharedJarsDir : jarCacheDir;
    Path destJarFile = new Path(fs.makeQualified(uploadDir), localJar.getPath().getName());
    return destJarFile;
  }
  /**
   * Upload a jar file to HDFS with retries to handle already existing jars
   * @param fs
   * @param localJar
   * @param destJarFile
   * @param jarFileMaximumRetry
   * @return
   * @throws IOException
   */
  public static boolean uploadJarToHdfs(FileSystem fs, FileStatus localJar, int jarFileMaximumRetry, Path destJarFile) throws IOException {
    int retryCount = 0;
    while (!fs.exists(destJarFile) || fs.getFileStatus(destJarFile).getLen() != localJar.getLen()) {
      try {
        if (fs.exists(destJarFile) && fs.getFileStatus(destJarFile).getLen() != localJar.getLen()) {
          Thread.sleep(WAITING_TIME_ON_IMCOMPLETE_UPLOAD);
          throw new IOException("Waiting for file to complete on uploading ... ");
        }
        // Set the first parameter as false for not deleting sourceFile
        // Set the second parameter as false for not overwriting existing file on the target, by default it is true.
        // If the file is preExisted but overwrite flag set to false, then an IOException if thrown.
        fs.copyFromLocalFile(false, false, localJar.getPath(), destJarFile);
      } catch (IOException | InterruptedException e) {
        log.warn("Path:" + destJarFile + " is not copied successfully. Will require retry.");
        retryCount += 1;
        if (retryCount >= jarFileMaximumRetry) {
          log.error("The jar file:" + destJarFile + "failed in being copied into hdfs", e);
          // If retry reaches upper limit, skip copying this file.
          return false;
        }
      }
    }
    return true;
  }
}
