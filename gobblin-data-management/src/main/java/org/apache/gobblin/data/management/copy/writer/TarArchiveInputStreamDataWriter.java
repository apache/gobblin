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

package org.apache.gobblin.data.management.copy.writer;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.util.io.StreamCopier;
import org.apache.gobblin.util.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPInputStream;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * An {@link FileAwareInputStreamDataWriter} to write archived {@link InputStream}s. The {@link #write(FileAwareInputStream)}
 * method receives a {@link GZIPInputStream} and converts it to a {@link TarArchiveInputStream}. Each
 * {@link TarArchiveEntry} is then written to the {@link FileSystem}.
 */
@Slf4j
public class TarArchiveInputStreamDataWriter extends FileAwareInputStreamDataWriter {

  public TarArchiveInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    super(state, numBranches, branchId);
  }

  /**
   * Untars the passed in {@link FileAwareInputStream} to the task's staging directory. Uses the name of the root
   * {@link TarArchiveEntry} in the stream as the directory name for the untarred file. The method also commits the data
   * by moving the file from staging to output directory.
   *
   * @see org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter#write(org.apache.gobblin.data.management.copy.FileAwareInputStream)
   */
  @Override
  public void writeImpl(InputStream inputStream, Path writeAt, CopyableFile copyableFile) throws IOException {
    this.closer.register(inputStream);

    TarArchiveInputStream tarIn = new TarArchiveInputStream(inputStream);
    final ReadableByteChannel inputChannel = Channels.newChannel(tarIn);
    TarArchiveEntry tarEntry;

    // flush the first entry in the tar, which is just the root directory
    tarEntry = tarIn.getNextTarEntry();
    String tarEntryRootName = StringUtils.remove(tarEntry.getName(), Path.SEPARATOR);

    log.info("Unarchiving at " + writeAt);

    try {
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {

        // the API tarEntry.getName() is misleading, it is actually the path of the tarEntry in the tar file
        String newTarEntryPath = tarEntry.getName().replace(tarEntryRootName, writeAt.getName());
        Path tarEntryStagingPath = new Path(writeAt.getParent(), newTarEntryPath);

        if (tarEntry.isDirectory() && !this.fs.exists(tarEntryStagingPath)) {
          this.fs.mkdirs(tarEntryStagingPath);
        } else if (!tarEntry.isDirectory()) {
          FSDataOutputStream out = this.fs.create(tarEntryStagingPath, true);
          final WritableByteChannel outputChannel = Channels.newChannel(out);
          try {
            StreamCopier copier = new StreamCopier(inputChannel, outputChannel);
            if (isInstrumentationEnabled()) {
              copier.withCopySpeedMeter(this.copySpeedMeter);
            }
            this.bytesWritten.addAndGet(copier.copy());
            if (isInstrumentationEnabled()) {
              log.info("File {}: copied {} bytes, average rate: {} B/s", copyableFile.getOrigin().getPath(), this.copySpeedMeter.getCount(), this.copySpeedMeter.getMeanRate());
            } else {
              log.info("File {} copied.", copyableFile.getOrigin().getPath());
            }
          } finally {
            out.close();
            outputChannel.close();
          }
        }
      }
    } finally {
      tarIn.close();
      inputChannel.close();
      inputStream.close();
    }
  }
}
