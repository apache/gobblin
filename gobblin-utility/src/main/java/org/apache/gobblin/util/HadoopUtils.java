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

package org.apache.gobblin.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.io.BaseEncoding;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.deprecation.DeprecationUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;
import org.apache.gobblin.writer.DataWriter;


/**
 * A utility class for working with Hadoop.
 */
@Slf4j
public class HadoopUtils {

  public static final String HDFS_ILLEGAL_TOKEN_REGEX = "[\\s:\\\\]";

  /**
   * A {@link Collection} of all known {@link FileSystem} schemes that do not support atomic renames or copies.
   *
   * <p>
   *   The following important properties are useful to remember when writing code that is compatible with S3:
   *   <ul>
   *     <li>Renames are not atomic, and require copying the entire source file to the destination file</li>
   *     <li>Writes to S3 using {@link FileSystem#create(Path)} will first go to the local filesystem, when the stream
   *     is closed the local file will be uploaded to S3</li>
   *   </ul>
   * </p>
   */
  public static final Collection<String> FS_SCHEMES_NON_ATOMIC =
      ImmutableSortedSet.orderedBy(String.CASE_INSENSITIVE_ORDER).add("s3").add("s3a").add("s3n").build();
  public static final String MAX_FILESYSTEM_QPS = "filesystem.throttling.max.filesystem.qps";
  private static final List<String> DEPRECATED_KEYS = Lists.newArrayList("gobblin.copy.max.filesystem.qps");
  private static final int MAX_RENAME_TRIES = 3;

  public static Configuration newConfiguration() {
    Configuration conf = new Configuration();

    // Explicitly check for S3 environment variables, so that Hadoop can access s3 and s3n URLs.
    // h/t https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala
    String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (awsAccessKeyId != null && awsSecretAccessKey != null) {
      conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
      conf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey);
    }

    // Add a new custom filesystem mapping
    conf.set("fs.sftp.impl", "org.apache.gobblin.source.extractor.extract.sftp.SftpLightWeightFileSystem");
    conf.set("fs.sftp.impl.disable.cache", "true");
    return conf;
  }

  /**
   * @deprecated Use {@link FileListUtils#listFilesRecursively(FileSystem, Path)}.
   */
  @Deprecated
  public static List<FileStatus> listStatusRecursive(FileSystem fileSystem, Path path) throws IOException {
    List<FileStatus> results = Lists.newArrayList();
    walk(results, fileSystem, path);
    return results;
  }

  /**
   * Get the path as a string without schema or authority.
   *
   * E.g. Converts sftp://user/data/file.txt to /user/data/file.txt
   */
  public static String toUriPath(Path path) {
    return path.toUri().getPath();
  }

  /**
   * A wrapper around {@link FileSystem#delete(Path, boolean)} which throws {@link IOException} if the given
   * {@link Path} exists, and {@link FileSystem#delete(Path, boolean)} returns False.
   */
  public static void deletePath(FileSystem fs, Path f, boolean recursive) throws IOException {
    if (fs.exists(f) && !fs.delete(f, recursive)) {
      throw new IOException("Failed to delete: " + f);
    }
  }

  /**
   * Calls deletePath() on each directory in the given list of directories to delete.
   * If moveToTrash is set, it will be moved to trash according to the file system trash policy.
   */
  public static void deleteDirectories(FileSystem fs, List<String> directoriesToDelete, boolean recursive, boolean moveToTrash) throws IOException {
    for (String directory : directoriesToDelete) {
      if (moveToTrash) {
        moveToTrash(fs, new Path(directory));
      } else {
        deletePath(fs, new Path(directory), recursive);
      }
    }
  }

  /**
   * A wrapper around {@link FileSystem#delete(Path, boolean)} that only deletes a given {@link Path} if it is present
   * on the given {@link FileSystem}.
   */
  public static void deleteIfExists(FileSystem fs, Path path, boolean recursive) throws IOException {
    if (fs.exists(path)) {
      deletePath(fs, path, recursive);
    }
  }

  public static void deletePathAndEmptyAncestors(FileSystem fs, Path f, boolean recursive) throws IOException {
    deletePath(fs, f, recursive);
    Path parent = f.getParent();
    while (parent != null) {
      if (fs.exists(parent) && fs.listStatus(parent).length == 0) {
        deletePath(fs, parent, true);
        parent = parent.getParent();
      } else {
        break;
      }
    }
  }

  /**
   * Delete files according to the regular expression provided
   * @param fs Filesystem object
   * @param path base path
   * @param regex regular expression to select files to delete
   * @throws IOException
   */
  public static void deletePathByRegex(FileSystem fs, final Path path, final String regex) throws IOException {
    FileStatus[] statusList = fs.listStatus(path, path1 -> path1.getName().matches(regex));

    for (final FileStatus oldJobFile : statusList) {
      HadoopUtils.deletePath(fs, oldJobFile.getPath(), true);
    }
  }

  /**
   * Moves the object to the filesystem trash according to the file system policy.
   * @param fs FileSystem object
   * @param path Path to the object to be moved to trash.
   * @throws IOException
   */
  public static void moveToTrash(FileSystem fs, Path path) throws IOException {
    Trash trash = new Trash(fs, new Configuration());
    trash.moveToTrash(path);
  }
  /**
   * Renames a src {@link Path} on fs {@link FileSystem} to a dst {@link Path}. If fs is a {@link LocalFileSystem} and
   * src is a directory then {@link File#renameTo} is called directly to avoid a directory rename race condition where
   * {@link org.apache.hadoop.fs.RawLocalFileSystem#rename} copies the conflicting src directory into dst resulting in
   * an extra nested level, such as /root/a/b/c/e/e where e is repeated.
   *
   * @param fs the {@link FileSystem} where the src {@link Path} exists
   * @param src the source {@link Path} which will be renamed
   * @param dst the {@link Path} to rename to
   * @return true if rename succeeded, false if rename failed.
   * @throws IOException if rename failed for reasons other than target exists.
   */
  public static boolean renamePathHandleLocalFSRace(FileSystem fs, Path src, Path dst) throws IOException {
    if (DecoratorUtils.resolveUnderlyingObject(fs) instanceof LocalFileSystem && fs.isDirectory(src)) {
      LocalFileSystem localFs = (LocalFileSystem) DecoratorUtils.resolveUnderlyingObject(fs);
      File srcFile = localFs.pathToFile(src);
      File dstFile = localFs.pathToFile(dst);

      return srcFile.renameTo(dstFile);
    }
    else {
      return fs.rename(src, dst);
    }
  }

  /**
   * A wrapper around {@link FileSystem#rename(Path, Path)} which throws {@link IOException} if
   * {@link FileSystem#rename(Path, Path)} returns False.
   */
  public static void renamePath(FileSystem fs, Path oldName, Path newName) throws IOException {
    renamePath(fs, oldName, newName, false);
  }

  /**
   * A wrapper around {@link FileSystem#rename(Path, Path)} which throws {@link IOException} if
   * {@link FileSystem#rename(Path, Path)} returns False.
   */
  public static void renamePath(FileSystem fs, Path oldName, Path newName, boolean overwrite) throws IOException {
    if (!fs.exists(oldName)) {
      throw new FileNotFoundException(String.format("Failed to rename %s to %s: src not found", oldName, newName));
    }
    if (fs.exists(newName)) {
      if (overwrite) {
        HadoopUtils.moveToTrash(fs, newName);
      } else {
        throw new FileAlreadyExistsException(
            String.format("Failed to rename %s to %s: dst already exists", oldName, newName));
      }
    }
    if (!fs.rename(oldName, newName)) {
      throw new IOException(String.format("Failed to rename %s to %s", oldName, newName));
    }
  }

  /**
   * Moves a src {@link Path} from a srcFs {@link FileSystem} to a dst {@link Path} on a dstFs {@link FileSystem}. If
   * the srcFs and the dstFs have the same scheme, and neither of them or S3 schemes, then the {@link Path} is simply
   * renamed. Otherwise, the data is from the src {@link Path} to the dst {@link Path}. So this method can handle copying
   * data between different {@link FileSystem} implementations.
   *
   * @param srcFs the source {@link FileSystem} where the src {@link Path} exists
   * @param src the source {@link Path} which will me moved
   * @param dstFs the destination {@link FileSystem} where the dst {@link Path} should be created
   * @param dst the {@link Path} to move data to
   */
  public static void movePath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, Configuration conf)
      throws IOException {

    movePath(srcFs, src, dstFs, dst, false, conf);
  }

  /**
   * Moves a src {@link Path} from a srcFs {@link FileSystem} to a dst {@link Path} on a dstFs {@link FileSystem}. If
   * the srcFs and the dstFs have the same scheme, and neither of them or S3 schemes, then the {@link Path} is simply
   * renamed. Otherwise, the data is from the src {@link Path} to the dst {@link Path}. So this method can handle copying
   * data between different {@link FileSystem} implementations.
   *
   * @param srcFs the source {@link FileSystem} where the src {@link Path} exists
   * @param src the source {@link Path} which will me moved
   * @param dstFs the destination {@link FileSystem} where the dst {@link Path} should be created
   * @param dst the {@link Path} to move data to
   * @param overwrite true if the destination should be overwritten; otherwise, false
   */
  public static void movePath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, boolean overwrite,
      Configuration conf) throws IOException {

    if (srcFs.getUri().getScheme().equals(dstFs.getUri().getScheme())
        && !FS_SCHEMES_NON_ATOMIC.contains(srcFs.getUri().getScheme())
        && !FS_SCHEMES_NON_ATOMIC.contains(dstFs.getUri().getScheme())) {
      renamePath(srcFs, src, dst);
    } else {
      copyPath(srcFs, src, dstFs, dst, true, overwrite, conf);
    }
  }

  /**
   * Copies data from a src {@link Path} to a dst {@link Path}.
   *
   * <p>
   *   This method should be used in preference to
   *   {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, boolean, Configuration)}, which does not handle
   *   clean up of incomplete files if there is an error while copying data.
   * </p>
   *
   * <p>
   *   TODO this method does not handle cleaning up any local files leftover by writing to S3.
   * </p>
   *
   * @param srcFs the source {@link FileSystem} where the src {@link Path} exists
   * @param src the {@link Path} to copy from the source {@link FileSystem}
   * @param dstFs the destination {@link FileSystem} where the dst {@link Path} should be created
   * @param dst the {@link Path} to copy data to
   */
  public static void copyPath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, Configuration conf)
      throws IOException {

    copyPath(srcFs, src, dstFs, dst, false, false, conf);
  }

  /**
   * Copies data from a src {@link Path} to a dst {@link Path}.
   *
   * <p>
   *   This method should be used in preference to
   *   {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, boolean, Configuration)}, which does not handle
   *   clean up of incomplete files if there is an error while copying data.
   * </p>
   *
   * <p>
   *   TODO this method does not handle cleaning up any local files leftover by writing to S3.
   * </p>
   *
   * @param srcFs the source {@link FileSystem} where the src {@link Path} exists
   * @param src the {@link Path} to copy from the source {@link FileSystem}
   * @param dstFs the destination {@link FileSystem} where the dst {@link Path} should be created
   * @param dst the {@link Path} to copy data to
   * @param overwrite true if the destination should be overwritten; otherwise, false
   */
  public static void copyPath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, boolean overwrite,
      Configuration conf) throws IOException {

    copyPath(srcFs, src, dstFs, dst, false, overwrite, conf);
  }

  private static void copyPath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, boolean deleteSource,
      boolean overwrite, Configuration conf) throws IOException {

    Preconditions.checkArgument(srcFs.exists(src),
        String.format("Cannot copy from %s to %s because src does not exist", src, dst));
    Preconditions.checkArgument(overwrite || !dstFs.exists(dst),
        String.format("Cannot copy from %s to %s because dst exists", src, dst));

    try {
      boolean isSourceFileSystemLocal = srcFs instanceof LocalFileSystem || srcFs instanceof RawLocalFileSystem;
      if (isSourceFileSystemLocal) {
        try {
          dstFs.copyFromLocalFile(deleteSource, overwrite, src, dst);
        } catch (IOException e) {
          throw new IOException(String.format("Failed to copy %s to %s", src, dst), e);
        }
      } else if (!FileUtil.copy(srcFs, src, dstFs, dst, deleteSource, overwrite, conf)) {
        throw new IOException(String.format("Failed to copy %s to %s", src, dst));
      }
    } catch (Throwable t1) {
      try {
        deleteIfExists(dstFs, dst, true);
      } catch (Throwable t2) {
        // Do nothing
      }
      throw t1;
    }
  }

  /**
   * Copies a src {@link Path} from a srcFs {@link FileSystem} to a dst {@link Path} on a dstFs {@link FileSystem}. If
   * either the srcFs or dstFs are S3 {@link FileSystem}s (as dictated by {@link #FS_SCHEMES_NON_ATOMIC}) then data is directly
   * copied from the src to the dst. Otherwise data is first copied to a tmp {@link Path}, which is then renamed to the
   * dst.
   *
   * @param srcFs the source {@link FileSystem} where the src {@link Path} exists
   * @param src the {@link Path} to copy from the source {@link FileSystem}
   * @param dstFs the destination {@link FileSystem} where the dst {@link Path} should be created
   * @param dst the {@link Path} to copy data to
   * @param tmp the temporary {@link Path} to use when copying data
   * @param overwriteDst true if the destination and tmp path should should be overwritten, false otherwise
   */
  public static void copyFile(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, Path tmp, boolean overwriteDst,
      Configuration conf) throws IOException {

    Preconditions.checkArgument(srcFs.isFile(src),
        String.format("Cannot copy from %s to %s because src is not a file", src, dst));

    if (FS_SCHEMES_NON_ATOMIC.contains(srcFs.getUri().getScheme())
        || FS_SCHEMES_NON_ATOMIC.contains(dstFs.getUri().getScheme())) {
      copyFile(srcFs, src, dstFs, dst, overwriteDst, conf);
    } else {
      copyFile(srcFs, src, dstFs, tmp, overwriteDst, conf);
      try {
        boolean renamed = false;
        if (overwriteDst && dstFs.exists(dst)) {
          try {
            deletePath(dstFs, dst, true);
          } finally {
            renamePath(dstFs, tmp, dst);
            renamed = true;
          }
        }
        if (!renamed) {
          renamePath(dstFs, tmp, dst);
        }
      } finally {
        deletePath(dstFs, tmp, true);
      }
    }
  }

  /**
   * Copy a file from a srcFs {@link FileSystem} to a dstFs {@link FileSystem}. The src {@link Path} must be a file,
   * that is {@link FileSystem#isFile(Path)} must return true for src.
   *
   * <p>
   *   If overwrite is specified to true, this method may delete the dst directory even if the copy from src to dst fails.
   * </p>
   *
   * @param srcFs the src {@link FileSystem} to copy the file from
   * @param src the src {@link Path} to copy
   * @param dstFs the destination {@link FileSystem} to write to
   * @param dst the destination {@link Path} to write to
   * @param overwrite true if the dst {@link Path} should be overwritten, false otherwise
   */
  public static void copyFile(FileSystem srcFs, Path src, FileSystem dstFs, Path dst, boolean overwrite,
      Configuration conf) throws IOException {

    Preconditions.checkArgument(srcFs.isFile(src),
        String.format("Cannot copy from %s to %s because src is not a file", src, dst));
    Preconditions.checkArgument(overwrite || !dstFs.exists(dst),
        String.format("Cannot copy from %s to %s because dst exists", src, dst));

    try (InputStream in = srcFs.open(src); OutputStream out = dstFs.create(dst, overwrite)) {
      IOUtils.copyBytes(in, out, conf, false);
    } catch (Throwable t1) {
      try {
        deleteIfExists(dstFs, dst, true);
      } catch (Throwable t2) {
        // Do nothing
      }
      throw t1;
    }
  }

  private static void walk(List<FileStatus> results, FileSystem fileSystem, Path path) throws IOException {
    for (FileStatus status : fileSystem.listStatus(path)) {
      if (!status.isDirectory()) {
        results.add(status);
      } else {
        walk(results, fileSystem, status.getPath());
      }
    }
  }

  /**
   * This method is an additive implementation of the {@link FileSystem#rename(Path, Path)} method. It moves all the
   * files/directories under 'from' path to the 'to' path without overwriting existing directories in the 'to' path.
   *
   * <p>
   * The rename operation happens at the first non-existent sub-directory. If a directory at destination path already
   * exists, it recursively tries to move sub-directories. If all the sub-directories also exist at the destination,
   * a file level move is done
   * </p>
   *
   * @param fileSystem on which the data needs to be moved
   * @param from path of the data to be moved
   * @param to path of the data to be moved
   */
  public static void renameRecursively(FileSystem fileSystem, Path from, Path to) throws IOException {

    log.info(String.format("Recursively renaming %s in %s to %s.", from, fileSystem.getUri(), to));

    FileSystem throttledFS = getOptionallyThrottledFileSystem(fileSystem, 10000);

    ExecutorService executorService = ScalingThreadPoolExecutor.newScalingThreadPool(1, 100, 100,
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("rename-thread-%d")));
    Queue<Future<?>> futures = Queues.newConcurrentLinkedQueue();

    try {
      if (!fileSystem.exists(from)) {
        throw new IOException("Trying to rename a path that does not exist! " + from);
      }

      futures.add(executorService
          .submit(new RenameRecursively(throttledFS, fileSystem.getFileStatus(from), to, executorService, futures)));
      int futuresUsed = 0;
      while (!futures.isEmpty()) {
        try {
          futures.poll().get();
          futuresUsed++;
        } catch (ExecutionException | InterruptedException ee) {
          throw new IOException(ee.getCause());
        }
      }

      log.info(String.format("Recursive renaming of %s to %s. (details: used %d futures)", from, to, futuresUsed));

    } finally {
      ExecutorsUtils.shutdownExecutorService(executorService, Optional.of(log), 1, TimeUnit.SECONDS);
    }
  }

  /**
   * Calls {@link #getOptionallyThrottledFileSystem(FileSystem, int)} parsing the qps from the input {@link State}
   * at key {@link #MAX_FILESYSTEM_QPS}.
   * @throws IOException
   */
  public static FileSystem getOptionallyThrottledFileSystem(FileSystem fs, State state) throws IOException {
    DeprecationUtils.renameDeprecatedKeys(state, MAX_FILESYSTEM_QPS, DEPRECATED_KEYS);

    if (state.contains(MAX_FILESYSTEM_QPS)) {
      return getOptionallyThrottledFileSystem(fs, state.getPropAsInt(MAX_FILESYSTEM_QPS));
    }
    return fs;
  }

  /**
   * Get a throttled {@link FileSystem} that limits the number of queries per second to a {@link FileSystem}. If
   * the input qps is <= 0, no such throttling will be performed.
   * @throws IOException
   */
  public static FileSystem getOptionallyThrottledFileSystem(FileSystem fs, int qpsLimit) throws IOException {
    if (fs instanceof Decorator) {
      for (Object obj : DecoratorUtils.getDecoratorLineage(fs)) {
        if (obj instanceof RateControlledFileSystem) {
          // Already rate controlled
          return fs;
        }
      }
    }

    if (qpsLimit > 0) {
      try {
        RateControlledFileSystem newFS = new RateControlledFileSystem(fs, qpsLimit);
        newFS.startRateControl();
        return newFS;
      } catch (ExecutionException ee) {
        throw new IOException("Could not create throttled FileSystem.", ee);
      }
    }
    return fs;
  }

  @AllArgsConstructor
  private static class RenameRecursively implements Runnable {

    private final FileSystem fileSystem;
    private final FileStatus from;
    private final Path to;
    private final ExecutorService executorService;
    private final Queue<Future<?>> futures;

    @Override
    public void run() {
      try {

        // Attempt to move safely if directory, unsafely if file (for performance, files are much less likely to collide on target)
        boolean moveSucessful =
            this.from.isDirectory() ? safeRenameIfNotExists(this.fileSystem, this.from.getPath(), this.to)
                : unsafeRenameIfNotExists(this.fileSystem, this.from.getPath(), this.to);

        if (!moveSucessful) {
          if (this.from.isDirectory()) {
            for (FileStatus fromFile : this.fileSystem.listStatus(this.from.getPath())) {
              Path relativeFilePath = new Path(StringUtils.substringAfter(fromFile.getPath().toString(),
                  this.from.getPath().toString() + Path.SEPARATOR));
              Path toFilePath = new Path(this.to, relativeFilePath);
              this.futures.add(this.executorService.submit(
                  new RenameRecursively(this.fileSystem, fromFile, toFilePath, this.executorService, this.futures)));
            }
          } else {
            log.info(String.format("File already exists %s. Will not rewrite", this.to));
          }

        }

      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Renames from to to if to doesn't exist in a thread-safe way. This method is necessary because
   * {@link FileSystem#rename} is inconsistent across file system implementations, e.g. in some of them rename(foo, bar)
   * will create bar/foo if bar already existed, but it will only create bar if it didn't.
   *
   * <p>
   *   The thread-safety is only guaranteed among calls to this method. An external modification to the relevant
   *   target directory could still cause unexpected results in the renaming.
   * </p>
   *
   * @param fs filesystem where rename will be executed.
   * @param from origin {@link Path}.
   * @param to target {@link Path}.
   * @return true if rename succeeded, false if the target already exists.
   * @throws IOException if rename failed for reasons other than target exists.
   */
  public synchronized static boolean safeRenameIfNotExists(FileSystem fs, Path from, Path to) throws IOException {
    return unsafeRenameIfNotExists(fs, from, to);
  }

  /**
   * Renames from to to if to doesn't exist in a non-thread-safe way.
   *
   * @param fs filesystem where rename will be executed.
   * @param from origin {@link Path}.
   * @param to target {@link Path}.
   * @return true if rename succeeded, false if the target already exists.
   * @throws IOException if rename failed for reasons other than target exists.
   */
  public static boolean unsafeRenameIfNotExists(FileSystem fs, Path from, Path to) throws IOException {
    if (!fs.exists(to)) {
      if (!fs.exists(to.getParent())) {
        fs.mkdirs(to.getParent());
      }

      if (!renamePathHandleLocalFSRace(fs, from, to)) {
        if (!fs.exists(to)) {
          throw new IOException(String.format("Failed to rename %s to %s.", from, to));
        }

        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * A thread safe variation of {@link #renamePath(FileSystem, Path, Path)} which can be used in
   * multi-threaded/multi-mapper environment. The rename operation always happens at file level hence directories are
   * not overwritten under the 'to' path.
   *
   * <p>
   * If the contents of destination 'to' path is not expected to be modified concurrently, use
   * {@link #renamePath(FileSystem, Path, Path)} which is faster and more optimized
   * </p>
   *
   * <b>NOTE: This does not seem to be working for all {@link FileSystem} implementations. Use
   * {@link #renameRecursively(FileSystem, Path, Path)}</b>
   *
   * @param fileSystem on which the data needs to be moved
   * @param from path of the data to be moved
   * @param to path of the data to be moved
   *
   */
  public static void safeRenameRecursively(FileSystem fileSystem, Path from, Path to) throws IOException {

    for (FileStatus fromFile : FileListUtils.listFilesRecursively(fileSystem, from)) {

      Path relativeFilePath =
          new Path(StringUtils.substringAfter(fromFile.getPath().toString(), from.toString() + Path.SEPARATOR));

      Path toFilePath = new Path(to, relativeFilePath);

      if (!fileSystem.exists(toFilePath)) {
        boolean renamed = false;

        // underlying file open can fail with file not found error due to some race condition
        // when the parent directory is created in another thread, so retry a few times
        for (int i = 0; !renamed && i < MAX_RENAME_TRIES; i++) {
          try {
            renamed = fileSystem.rename(fromFile.getPath(), toFilePath);
            break;
          } catch (FileNotFoundException e) {
            if (i + 1 >= MAX_RENAME_TRIES) {
              throw e;
            }
          }
        }

        if (!renamed) {
          throw new IOException(String.format("Failed to rename %s to %s.", fromFile.getPath(), toFilePath));
        }
        log.info(String.format("Renamed %s to %s", fromFile.getPath(), toFilePath));
      } else {
        log.info(String.format("File already exists %s. Will not rewrite", toFilePath));
      }
    }
  }

  public static Configuration getConfFromState(State state) {
    return getConfFromState(state, Optional.<String> absent());
  }

  /**
   * Provides Hadoop configuration given state.
   * It also supports decrypting values on "encryptedPath".
   * Note that this encryptedPath path will be removed from full path of each config key and leaving only child path on the key(s).
   * If there's same config path as child path, the one stripped will have higher priority.
   *
   * e.g:
   * - encryptedPath: writer.fs.encrypted
   *   before: writer.fs.encrypted.secret
   *   after: secret
   *
   * Common use case for these encryptedPath:
   *   When there's have encrypted credential in job property but you'd like Filesystem to get decrypted value.
   *
   * @param srcConfig source config.
   * @param encryptedPath Optional. If provided, config that is on this path will be decrypted. @see ConfigUtils.resolveEncrypted
   *                      Note that config on encryptedPath will be included in the end result even it's not part of includeOnlyPath
   * @return Hadoop Configuration.
   */
  public static Configuration getConfFromState(State state, Optional<String> encryptedPath) {
    Config config = ConfigFactory.parseProperties(state.getProperties());
    if (encryptedPath.isPresent()) {
      config = ConfigUtils.resolveEncrypted(config, encryptedPath);
    }
    Configuration conf = newConfiguration();

    for (Entry<String, ConfigValue> entry : config.entrySet()) {
      conf.set(entry.getKey(), entry.getValue().unwrapped().toString());
    }
    return conf;
  }

  public static Configuration getConfFromProperties(Properties properties) {
    Configuration conf = newConfiguration();
    for (String propName : properties.stringPropertyNames()) {
      conf.set(propName, properties.getProperty(propName));
    }
    return conf;
  }

  public static State getStateFromConf(Configuration conf) {
    State state = new State();
    for (Entry<String, String> entry : conf) {
      state.setProp(entry.getKey(), entry.getValue());
    }
    return state;
  }

  /**
   * Set the group associated with a given path.
   *
   * @param fs the {@link FileSystem} instance used to perform the file operation
   * @param path the given path
   * @param group the group associated with the path
   * @throws IOException
   */
  public static void setGroup(FileSystem fs, Path path, String group) throws IOException {
    fs.setOwner(path, fs.getFileStatus(path).getOwner(), group);
  }

  /**
   * Serialize a {@link Writable} object into a string.
   *
   * @param writable the {@link Writable} object to be serialized
   * @return a string serialized from the {@link Writable} object
   * @throws IOException if there's something wrong with the serialization
   */
  public static String serializeToString(Writable writable) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      writable.write(dataOutputStream);
      return BaseEncoding.base64().encode(byteArrayOutputStream.toByteArray());
    }
  }

  /**
   * Deserialize a {@link Writable} object from a string.
   *
   * @param writableClass the {@link Writable} implementation class
   * @param serializedWritableStr the string containing a serialized {@link Writable} object
   * @return a {@link Writable} deserialized from the string
   * @throws IOException if there's something wrong with the deserialization
   */
  public static Writable deserializeFromString(Class<? extends Writable> writableClass, String serializedWritableStr)
      throws IOException {
    return deserializeFromString(writableClass, serializedWritableStr, new Configuration());
  }

  /**
   * Deserialize a {@link Writable} object from a string.
   *
   * @param writableClass the {@link Writable} implementation class
   * @param serializedWritableStr the string containing a serialized {@link Writable} object
   * @param configuration a {@link Configuration} object containing Hadoop configuration properties
   * @return a {@link Writable} deserialized from the string
   * @throws IOException if there's something wrong with the deserialization
   */
  public static Writable deserializeFromString(Class<? extends Writable> writableClass, String serializedWritableStr,
      Configuration configuration) throws IOException {
    byte[] writableBytes = BaseEncoding.base64().decode(serializedWritableStr);

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(writableBytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      Writable writable = ReflectionUtils.newInstance(writableClass, configuration);
      writable.readFields(dataInputStream);
      return writable;
    }
  }

  /**
   * Given a {@link FsPermission} objects, set a key, value pair in the given {@link State} for the writer to
   * use when creating files. This method should be used in conjunction with {@link #deserializeWriterFilePermissions(State, int, int)}.
   */
  public static void serializeWriterFilePermissions(State state, int numBranches, int branchId,
      FsPermission fsPermissions) {
    serializeFsPermissions(state,
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PERMISSIONS, numBranches, branchId),
        fsPermissions);
  }

  /**
   * Given a {@link FsPermission} objects, set a key, value pair in the given {@link State} for the writer to
   * use when creating files. This method should be used in conjunction with {@link #deserializeWriterDirPermissions(State, int, int)}.
   */
  public static void serializeWriterDirPermissions(State state, int numBranches, int branchId,
      FsPermission fsPermissions) {
    serializeFsPermissions(state,
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DIR_PERMISSIONS, numBranches, branchId),
        fsPermissions);
  }

  /**
   * Helper method that serializes a {@link FsPermission} object.
   */
  private static void serializeFsPermissions(State state, String key, FsPermission fsPermissions) {
    state.setProp(key, String.format("%04o", fsPermissions.toShort()));
  }

  /**
   * Given a {@link String} in octal notation, set a key, value pair in the given {@link State} for the writer to
   * use when creating files. This method should be used in conjunction with {@link #deserializeWriterFilePermissions(State, int, int)}.
   */
  public static void setWriterFileOctalPermissions(State state, int numBranches, int branchId,
      String octalPermissions) {
    state.setProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PERMISSIONS, numBranches, branchId),
        octalPermissions);
  }

  /**
   * Given a {@link String} in octal notation, set a key, value pair in the given {@link State} for the writer to
   * use when creating directories. This method should be used in conjunction with {@link #deserializeWriterDirPermissions(State, int, int)}.
   */
  public static void setWriterDirOctalPermissions(State state, int numBranches, int branchId, String octalPermissions) {
    state.setProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DIR_PERMISSIONS, numBranches, branchId),
        octalPermissions);
  }

  /**
   * Deserializes a {@link FsPermission}s object that should be used when a {@link DataWriter} is writing a file.
   */
  public static FsPermission deserializeWriterFilePermissions(State state, int numBranches, int branchId) {
    return new FsPermission(state.getPropAsShortWithRadix(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PERMISSIONS, numBranches, branchId),
        FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX));
  }

  /**
   * Deserializes a {@link FsPermission}s object that should be used when a {@link DataWriter} is creating directories.
   */
  public static FsPermission deserializeWriterDirPermissions(State state, int numBranches, int branchId) {
    return new FsPermission(state.getPropAsShortWithRadix(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DIR_PERMISSIONS, numBranches, branchId),
        FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX));
  }

  /**
   * Get {@link FsPermission} from a {@link State} object.
   *
   * @param props A {@link State} containing properties.
   * @param propName The property name for the permission. If not contained in the given state,
   * defaultPermission will be used.
   * @param defaultPermission default permission if propName is not contained in props.
   * @return An {@link FsPermission} object.
   */
  public static FsPermission deserializeFsPermission(State props, String propName, FsPermission defaultPermission) {
    short mode = props.getPropAsShortWithRadix(propName, defaultPermission.toShort(),
        ConfigurationKeys.PERMISSION_PARSING_RADIX);
    return new FsPermission(mode);
  }

  /**
   * Remove illegal HDFS path characters from the given path. Illegal characters will be replaced
   * with the given substitute.
   */
  public static String sanitizePath(String path, String substitute) {
    Preconditions.checkArgument(substitute.replaceAll(HDFS_ILLEGAL_TOKEN_REGEX, "").equals(substitute),
        "substitute contains illegal characters: " + substitute);

    return path.replaceAll(HDFS_ILLEGAL_TOKEN_REGEX, substitute);
  }

  /**
   * Remove illegal HDFS path characters from the given path. Illegal characters will be replaced
   * with the given substitute.
   */
  public static Path sanitizePath(Path path, String substitute) {
    return new Path(sanitizePath(path.toString(), substitute));
  }

  /**
   * Try to set owner and permissions for the path. Will not throw exception.
   */
  public static void setPermissions(Path location, Optional<String> owner, Optional<String> group, FileSystem fs,
      FsPermission permission) {
    try {
      if (!owner.isPresent()) {
        return;
      }
      if (!group.isPresent()) {
        return;
      }
      fs.setOwner(location, owner.get(), group.get());
      fs.setPermission(location, permission);
      if (!fs.isDirectory(location)) {
        return;
      }
      for (FileStatus fileStatus : fs.listStatus(location)) {
        setPermissions(fileStatus.getPath(), owner, group, fs, permission);
      }
    } catch (IOException e) {
      log.warn("Exception occurred while trying to change permissions : " + e.getMessage());
    }
  }

  public static boolean hasContent(FileSystem fs, Path path)
      throws IOException {
    if (!fs.isDirectory(path)) {
      return true;
    }
    boolean content = false;
    for (FileStatus fileStatus : fs.listStatus(path)) {
      content = content || hasContent(fs, fileStatus.getPath());
      if (content) {
        break;
      }
    }
    return content;
  }

  /**
   * Add "gobblin-site.xml" as a {@link Configuration} resource.
   */
  public static void addGobblinSite() {
    Configuration.addDefaultResource("gobblin-site.xml");
  }

  /**
   * Get a {@link FileSystem} object for the uri specified at {@link ConfigurationKeys#SOURCE_FILEBASED_FS_URI}.
   * @throws IOException
   */
  public static FileSystem getSourceFileSystem(State state) throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state, Optional.of(ConfigurationKeys.SOURCE_FILEBASED_ENCRYPTED_CONFIG_PATH));
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  /**
   * Get a {@link FileSystem} object for the uri specified at {@link ConfigurationKeys#WRITER_FILE_SYSTEM_URI}.
   * @throws IOException
   */
  public static FileSystem getWriterFileSystem(State state, int numBranches, int branchId)
      throws IOException {
    return HadoopUtils.getOptionallyThrottledFileSystem(WriterUtils.getWriterFS(state, numBranches, branchId), state);
  }
}
