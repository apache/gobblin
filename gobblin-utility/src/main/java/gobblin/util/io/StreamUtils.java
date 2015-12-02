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
package gobblin.util.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import gobblin.configuration.ConfigurationKeys;


/**
 * Utility class of input/output stream helpers.
 */
public class StreamUtils {

  private static final int KB = 1024;
  private static final int DEFAULT_BUFFER_SIZE = 32 * KB;

  /**
   * Convert an instance of {@link InputStream} to a {@link FSDataInputStream} that is {@link Seekable} and
   * {@link PositionedReadable}.
   *
   * @see SeekableFSInputStream
   *
   */
  public static FSDataInputStream convertStream(InputStream in) throws IOException {
    return new FSDataInputStream(new SeekableFSInputStream(in));
  }

  /**
   * Copies an {@link InputStream} to and {@link OutputStream} using {@link Channels}.
   *
   * <p>
   * <b>Note:</b> The method does not close the {@link InputStream} and {@link OutputStream}. However, the
   * {@link ReadableByteChannel} and {@link WritableByteChannel}s are closed
   * </p>
   *
   * @return Total bytes copied
   */
  public static long copy(InputStream is, OutputStream os) throws IOException {

    final ReadableByteChannel inputChannel = Channels.newChannel(is);
    final WritableByteChannel outputChannel = Channels.newChannel(os);

    long totalBytesCopied = copy(inputChannel, outputChannel);

    inputChannel.close();
    outputChannel.close();

    return totalBytesCopied;
  }

  /**
   * Copies a {@link ReadableByteChannel} to a {@link WritableByteChannel}.
   * <p>
   * <b>Note:</b> The {@link ReadableByteChannel} and {@link WritableByteChannel}s are NOT closed by the method
   * </p>
   *
   * @return Total bytes copied
   */
  public static long copy(ReadableByteChannel inputChannel, WritableByteChannel outputChannel) throws IOException {

    long bytesRead = 0;
    long totalBytesRead = 0;
    final ByteBuffer buffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
    while ((bytesRead = inputChannel.read(buffer)) != -1) {
      totalBytesRead += bytesRead;
      // flip the buffer to be written
      buffer.flip();
      outputChannel.write(buffer);
      // Clear if empty
      buffer.compact();
    }
    // Done writing, now flip to read again
    buffer.flip();
    // check that buffer is fully written.
    while (buffer.hasRemaining()) {
      outputChannel.write(buffer);
    }

    return totalBytesRead;
  }

  /**
   * Creates a tar gzip file using a given {@link Path} as input and a given {@link Path} as a destination. If the given
   * input is a file then only that file will be added to tarball. If it is a directory then the entire directory will
   * be recursively put into the tarball.
   *
   * @param fs the {@link FileSystem} the input exists, and the the output should be written to.
   * @param sourcePath the {@link Path} of the input files, this can either be a file or a directory.
   * @param destPath the {@link Path} that tarball should be written to.
   */
  public static void tar(FileSystem fs, Path sourcePath, Path destPath)
      throws IOException {
    tar(fs, fs, sourcePath, destPath);
  }

  /**
   * Similiar to {@link #tar(FileSystem, Path, Path)} except the source and destination {@link FileSystem} can be different.
   *
   * @see {@link #tar(FileSystem, Path, Path)}
   */
  public static void tar(FileSystem sourceFs, FileSystem destFs, Path sourcePath, Path destPath)
      throws IOException {
    try (FSDataOutputStream fsDataOutputStream = destFs.create(destPath);
        TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(
            new GzipCompressorOutputStream(fsDataOutputStream), ConfigurationKeys.DEFAULT_CHARSET_ENCODING.name())) {

      FileStatus fileStatus = sourceFs.getFileStatus(sourcePath);

      if (fileStatus.isDir()) {
        dirToTarArchiveOutputStreamRecursive(fileStatus, sourceFs, new File("."), tarArchiveOutputStream);
      } else {
        try (FSDataInputStream fsDataInputStream = sourceFs.open(sourcePath)) {
          fileToTarArchiveOutputStream(fileStatus, fsDataInputStream, new File(sourcePath.getName()),
              tarArchiveOutputStream);
        }
      }
    }
  }

  /**
   * Helper method for {@link #tar(FileSystem, FileSystem, Path, Path)} that recursively adds a directory to a given
   * {@link TarArchiveOutputStream}.
   */
  private static void dirToTarArchiveOutputStreamRecursive(FileStatus dirFileStatus, FileSystem fs, File destDir,
      TarArchiveOutputStream tarArchiveOutputStream)
      throws IOException {

    Preconditions.checkState(dirFileStatus.isDir());

    File dir = new File(destDir, dirFileStatus.getPath().getName());
    dirToTarArchiveOutputStream(dir, tarArchiveOutputStream);

    for (FileStatus childFileStatus : fs.listStatus(dirFileStatus.getPath())) {
      File childFile = new File(dir, childFileStatus.getPath().getName());

      if (childFileStatus.isDir()) {
        dirToTarArchiveOutputStreamRecursive(childFileStatus, fs, childFile, tarArchiveOutputStream);
      } else {
        try (FSDataInputStream fsDataInputStream = fs.open(childFileStatus.getPath())) {
          fileToTarArchiveOutputStream(childFileStatus, fsDataInputStream, childFile, tarArchiveOutputStream);
        }
      }
    }
  }

  /**
   * Helper method for {@link #tar(FileSystem, FileSystem, Path, Path)} that adds a directory entry to a given
   * {@link TarArchiveOutputStream}.
   */
  private static void dirToTarArchiveOutputStream(File destDir, TarArchiveOutputStream tarArchiveOutputStream)
      throws IOException {
    Preconditions.checkArgument(destDir.isDirectory());

    try {
      tarArchiveOutputStream.putArchiveEntry(new TarArchiveEntry(destDir));
    } finally {
      tarArchiveOutputStream.closeArchiveEntry();
    }
  }

  /**
   * Helper method for {@link #tar(FileSystem, FileSystem, Path, Path)} that adds a file entry to a given
   * {@link TarArchiveOutputStream} and copies the contents of the file to the new entry.
   */
  private static void fileToTarArchiveOutputStream(FileStatus fileStatus, FSDataInputStream fsDataInputStream,
      File destFile, TarArchiveOutputStream tarArchiveOutputStream)
      throws IOException {
    Preconditions.checkState(!fileStatus.isDir());
    Preconditions.checkState(destFile.isFile());

    tarArchiveOutputStream.putArchiveEntry(new TarArchiveEntry(destFile));

    try {
      IOUtils.copy(fsDataInputStream, tarArchiveOutputStream);
    } finally {
      tarArchiveOutputStream.closeArchiveEntry();
    }
  }
}
