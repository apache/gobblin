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

package gobblin.util;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.DataWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closer;


/**
 * A utility class for working with Hadoop.
 */
@Slf4j
public class HadoopUtils {

  public static final String HDFS_ILLEGAL_TOKEN_REGEX = "[\\s:\\\\]";

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
    conf.set("fs.sftp.impl", "gobblin.source.extractor.extract.sftp.SftpLightWeightFileSystem");
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
   * A wrapper around {@link FileSystem#rename(Path, Path)} which throws {@link IOException} if
   * {@link FileSystem#rename(Path, Path)} returns False.
   */
  public static void renamePath(FileSystem fs, Path oldName, Path newName) throws IOException {
    if (!fs.exists(oldName)) {
      throw new FileNotFoundException(String.format("Failed to rename %s to %s: src not found", oldName, newName));
    }
    if (fs.exists(newName)) {
      throw new FileAlreadyExistsException(
          String.format("Failed to rename %s to %s: dst already exists", oldName, newName));
    }
    if (!fs.rename(oldName, newName)) {
      throw new IOException(String.format("Failed to rename %s to %s", oldName, newName));
    }
  }

  /**
   * A wrapper around {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, Configuration)}
   * which throws {@link IOException}
   * if {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, Configuration)} returns false.
   */
  public static void copyPath(FileSystem fs, Path src, Path dst) throws IOException {
    if (!FileUtil.copy(fs, src, fs, dst, false, fs.getConf())) {
      throw new IOException(String.format("Failed to copy %s to %s", src, dst));
    }
  }

  /**
   * A wrapper around {@link HadoopUtils#renamePath(FileSystem, Path, Path)} and
   * {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, Configuration)} which will rename the path
   * if the src and dst filesystems are the same; otherwise, the src file will be moved the the dst filesystem.
   * An {@link IOException} if {@link FileUtil#copy(FileSystem, Path, FileSystem, Path, boolean, Configuration)} returns false.
   */
  public static void movePath(FileSystem srcFs, Path src, FileSystem dstFs, Path dst) throws IOException {
    if (srcFs.getUri().equals(dstFs.getUri())) {
      renamePath(srcFs, src, dst);
    } else {
      boolean isSourceFileSystemLocal = srcFs instanceof LocalFileSystem;
      FileStatus srcStatus = srcFs.getFileStatus(src);
      if (srcStatus.isDir()) {
        for (FileStatus srcFile : FileListUtils.listFilesRecursively(srcFs, src)) {
          if (isSourceFileSystemLocal) {
            Path dstFile = new Path(dst, srcFile.getPath().getName());
            dstFs.moveFromLocalFile(srcFile.getPath(), dstFile);
          } else {
            if (!FileUtil.copy(srcFs, src, dstFs, dst, true, false, dstFs.getConf())) {
              throw new IOException(String.format("Failed to move %s to %s", src, dst));
            }
          }
        }
      } else {
        if (isSourceFileSystemLocal) {
          dstFs.moveFromLocalFile(srcStatus.getPath(), dst);
        } else {
          if (!FileUtil.copy(srcFs, src, dstFs, dst, true, false, dstFs.getConf())) {
            throw new IOException(String.format("Failed to move %s to %s", src, dst));
          }
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static void walk(List<FileStatus> results, FileSystem fileSystem, Path path) throws IOException {
    for (FileStatus status : fileSystem.listStatus(path)) {
      if (!status.isDir()) {
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

    // Need this check for hadoop2
    if (!fileSystem.exists(from)) {
      return;
    }

    for (FileStatus fromFile : fileSystem.listStatus(from)) {

      Path relativeFilePath =
          new Path(StringUtils.substringAfter(fromFile.getPath().toString(), from.toString() + Path.SEPARATOR));

      Path toFilePath = new Path(to, relativeFilePath);

      if (!safeRenameIfNotExists(fileSystem, fromFile.getPath(), toFilePath)) {
        if(fromFile.isDir()) {
          renameRecursively(fileSystem, fromFile.getPath(), toFilePath);
        } else {
          log.info(String.format("File already exists %s. Will not rewrite", toFilePath));
        }
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
    if(!fs.exists(to)) {
      if(!fs.exists(to.getParent())) {
        fs.mkdirs(to.getParent());
      }
      if(!fs.rename(from, to)) {
        throw new IOException(String.format("Failed to rename %s to %s.", from, to));
      }
      return true;
    } else {
      return false;
    }
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
        if (!fileSystem.rename(fromFile.getPath(), toFilePath)) {
          throw new IOException(String.format("Failed to rename %s to %s.", fromFile.getPath(), toFilePath));
        } else {
          log.info(String.format("Renamed %s to %s", fromFile.getPath(), toFilePath));
        }
      } else {
        log.info(String.format("File already exists %s. Will not rewrite", toFilePath));
      }
    }
  }

  public static Configuration getConfFromState(State state) {
    Configuration conf = newConfiguration();
    for (String propName : state.getPropertyNames()) {
      conf.set(propName, state.getProp(propName));
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
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream byteArrayOutputStream = closer.register(new ByteArrayOutputStream());
      DataOutputStream dataOutputStream = closer.register(new DataOutputStream(byteArrayOutputStream));
      writable.write(dataOutputStream);
      return BaseEncoding.base64().encode(byteArrayOutputStream.toByteArray());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
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
    Closer closer = Closer.create();
    try {
      byte[] writableBytes = BaseEncoding.base64().decode(serializedWritableStr);
      ByteArrayInputStream byteArrayInputStream = closer.register(new ByteArrayInputStream(writableBytes));
      DataInputStream dataInputStream = closer.register(new DataInputStream(byteArrayInputStream));
      Writable writable = ReflectionUtils.newInstance(writableClass, configuration);
      writable.readFields(dataInputStream);
      return writable;
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
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
}
