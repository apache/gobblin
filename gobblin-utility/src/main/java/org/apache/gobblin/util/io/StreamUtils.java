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
package org.apache.gobblin.util.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * Utility class of input/output stream helpers.
 */
public class StreamUtils {

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
    return new StreamCopier(is, os).copy();
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
    return new StreamCopier(inputChannel, outputChannel).copy();
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
  public static void tar(FileSystem fs, Path sourcePath, Path destPath) throws IOException {
    tar(fs, fs, sourcePath, destPath);
  }

  /**
   * Similiar to {@link #tar(FileSystem, Path, Path)} except the source and destination {@link FileSystem} can be different.
   *
   * @see #tar(FileSystem, Path, Path)
   */
  public static void tar(FileSystem sourceFs, FileSystem destFs, Path sourcePath, Path destPath) throws IOException {
    try (FSDataOutputStream fsDataOutputStream = destFs.create(destPath);
        TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(
            new GzipCompressorOutputStream(fsDataOutputStream), ConfigurationKeys.DEFAULT_CHARSET_ENCODING.name())) {

      FileStatus fileStatus = sourceFs.getFileStatus(sourcePath);

      if (sourceFs.isDirectory(sourcePath)) {
        dirToTarArchiveOutputStreamRecursive(fileStatus, sourceFs, Optional.<Path> absent(), tarArchiveOutputStream);
      } else {
        try (FSDataInputStream fsDataInputStream = sourceFs.open(sourcePath)) {
          fileToTarArchiveOutputStream(fileStatus, fsDataInputStream, new Path(sourcePath.getName()),
              tarArchiveOutputStream);
        }
      }
    }
  }

  /**
   * Helper method for {@link #tar(FileSystem, FileSystem, Path, Path)} that recursively adds a directory to a given
   * {@link TarArchiveOutputStream}.
   */
  private static void dirToTarArchiveOutputStreamRecursive(FileStatus dirFileStatus, FileSystem fs,
      Optional<Path> destDir, TarArchiveOutputStream tarArchiveOutputStream) throws IOException {

    Preconditions.checkState(fs.isDirectory(dirFileStatus.getPath()));

    Path dir = destDir.isPresent() ? new Path(destDir.get(), dirFileStatus.getPath().getName())
        : new Path(dirFileStatus.getPath().getName());
    dirToTarArchiveOutputStream(dir, tarArchiveOutputStream);

    for (FileStatus childFileStatus : fs.listStatus(dirFileStatus.getPath())) {
      Path childFile = new Path(dir, childFileStatus.getPath().getName());

      if (fs.isDirectory(childFileStatus.getPath())) {
        dirToTarArchiveOutputStreamRecursive(childFileStatus, fs, Optional.of(childFile), tarArchiveOutputStream);
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
  private static void dirToTarArchiveOutputStream(Path destDir, TarArchiveOutputStream tarArchiveOutputStream)
      throws IOException {
    TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(formatPathToDir(destDir));
    tarArchiveEntry.setModTime(System.currentTimeMillis());
    tarArchiveOutputStream.putArchiveEntry(tarArchiveEntry);
    tarArchiveOutputStream.closeArchiveEntry();
  }

  /**
   * Helper method for {@link #tar(FileSystem, FileSystem, Path, Path)} that adds a file entry to a given
   * {@link TarArchiveOutputStream} and copies the contents of the file to the new entry.
   */
  private static void fileToTarArchiveOutputStream(FileStatus fileStatus, FSDataInputStream fsDataInputStream,
      Path destFile, TarArchiveOutputStream tarArchiveOutputStream) throws IOException {
    TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(formatPathToFile(destFile));
    tarArchiveEntry.setSize(fileStatus.getLen());
    tarArchiveEntry.setModTime(System.currentTimeMillis());
    tarArchiveOutputStream.putArchiveEntry(tarArchiveEntry);

    try {
      IOUtils.copy(fsDataInputStream, tarArchiveOutputStream);
    } finally {
      tarArchiveOutputStream.closeArchiveEntry();
    }
  }

  /**
   * Convert a {@link Path} to a {@link String} and make sure it is properly formatted to be recognized as a directory
   * by {@link TarArchiveEntry}.
   */
  private static String formatPathToDir(Path path) {
    return path.toString().endsWith(Path.SEPARATOR) ? path.toString() : path.toString() + Path.SEPARATOR;
  }

  /**
   * Convert a {@link Path} to a {@link String} and make sure it is properly formatted to be recognized as a file
   * by {@link TarArchiveEntry}.
   */
  private static String formatPathToFile(Path path) {
    return StringUtils.removeEnd(path.toString(), Path.SEPARATOR);
  }

  /*
   * Determines if a byte array is compressed. The java.util.zip GZip
   * implementation does not expose the GZip header so it is difficult to determine
   * if a string is compressed.
   * Copied from Helix GZipCompressionUtil
   * @param bytes an array of bytes
   * @return true if the array is compressed or false otherwise
   */
  public static boolean isCompressed(byte[] bytes) {
    if ((bytes == null) || (bytes.length < 2)) {
      return false;
    } else {
      return ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) &&
          (bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
    }
  }

  /**
   * Reads the full contents of a ByteBuffer and writes them to an OutputStream. The ByteBuffer is
   * consumed by this operation; eg in.remaining() will be 0 after it completes successfully.
   * @param in  ByteBuffer to write into the OutputStream
   * @param out Destination stream
   * @throws IOException If there is an error writing into the OutputStream
   */
  public static void byteBufferToOutputStream(ByteBuffer in, OutputStream out)
      throws IOException {
    final int BUF_SIZE = 8192;

    if (in.hasArray()) {
      out.write(in.array(), in.arrayOffset() + in.position(), in.remaining());
    } else {
      final byte[] b = new byte[Math.min(in.remaining(), BUF_SIZE)];
      while (in.remaining() > 0) {
        int bytesToRead = Math.min(in.remaining(), BUF_SIZE);
        in.get(b, 0, bytesToRead);

        out.write(b, 0, bytesToRead);
      }
    }
  }
}
