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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.codahale.metrics.Meter;

import org.apache.gobblin.util.limiter.Limiter;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * A class that copies an {@link InputStream} to an {@link OutputStream} in a configurable way.
 */
@NotThreadSafe
public class StreamCopier {

  private static final int KB = 1024;
  public static final int DEFAULT_BUFFER_SIZE = 32 * KB;

  private final ReadableByteChannel inputChannel;
  private final WritableByteChannel outputChannel;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private Meter copySpeedMeter;

  private boolean closeChannelsOnComplete = false;
  private volatile boolean copied = false;

  public StreamCopier(InputStream inputStream, OutputStream outputStream) {
    this(Channels.newChannel(inputStream), Channels.newChannel(outputStream));
  }

  public StreamCopier(ReadableByteChannel inputChannel, WritableByteChannel outputChannel) {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
  }

  /**
   * Set the size in bytes of the buffer used to copy.
   */
  public StreamCopier withBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  /**
   * Set a {@link Meter} where copy speed will be reported.
   */
  public StreamCopier withCopySpeedMeter(Meter copySpeedMeter) {
    this.copySpeedMeter = copySpeedMeter;
    return this;
  }

  /**
   * Close the input and output {@link java.nio.channels.Channel}s after copy, whether the copy was successful or not.
   */
  public StreamCopier closeChannelsOnComplete() {
    this.closeChannelsOnComplete = true;
    return this;
  }

  /**
   * Execute the copy of bytes from the input to the output stream.
   * Note: this method should only be called once. Further calls will throw a {@link IllegalStateException}.
   * @return Number of bytes copied.
   */
  public synchronized long copy() throws IOException {

    if (this.copied) {
      throw new IllegalStateException(String.format("%s already copied.", StreamCopier.class.getName()));
    }
    this.copied = true;

    try {
      long bytesRead = 0;
      long totalBytesRead = 0;

      final ByteBuffer buffer = ByteBuffer.allocateDirect(this.bufferSize);
      while ((bytesRead = fillBufferFromInputChannel(buffer)) != -1) {
        totalBytesRead += bytesRead;
        // flip the buffer to be written
        buffer.flip();
        this.outputChannel.write(buffer);
        // Clear if empty
        buffer.compact();
        if (this.copySpeedMeter != null) {
          this.copySpeedMeter.mark(bytesRead);
        }
      }
      // Done writing, now flip to read again
      buffer.flip();
      // check that buffer is fully written.
      while (buffer.hasRemaining()) {
        this.outputChannel.write(buffer);
      }

      return totalBytesRead;
    } finally {
      if (this.closeChannelsOnComplete) {
        this.inputChannel.close();
        this.outputChannel.close();
      }
    }
  }

  private long fillBufferFromInputChannel(ByteBuffer buffer) throws IOException {
    return this.inputChannel.read(buffer);
  }

  /**
   * Indicates there were not enough permits in the {@link Limiter} to finish the copy.
   */
  public static class NotEnoughPermitsException extends IOException {
    private NotEnoughPermitsException() {
      super("Not enough permits to perform stream copy.");
    }
  }

  private static Closeable NOOP_CLOSEABLE = new Closeable() {
    @Override
    public void close() throws IOException {
      // nothing to do
    }
  };
}
