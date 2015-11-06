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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;


/**
 * Utility class of input/output stream helpers.
 */
public class StreamUtils {

  private static final int KB = 1024;
  private static final int DEFAULT_BUFFER_SIZE = 8 * KB;

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
   */
  public static void copy(InputStream is, OutputStream os) throws IOException {

    final ReadableByteChannel inputChannel = Channels.newChannel(is);
    final WritableByteChannel outputChannel = Channels.newChannel(os);

    copy(inputChannel, outputChannel);

    inputChannel.close();
    outputChannel.close();

  }

  /**
   * Copies a {@link ReadableByteChannel} to a {@link WritableByteChannel}.
   * <p>
   * <b>Note:</b> The {@link ReadableByteChannel} and {@link WritableByteChannel}s are NOT closed by the method
   * </p>
   *
   */
  public static void copy(ReadableByteChannel inputChannel, WritableByteChannel outputChannel) throws IOException {

    final ByteBuffer buffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
    while (inputChannel.read(buffer) != -1) {
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
  }
}
