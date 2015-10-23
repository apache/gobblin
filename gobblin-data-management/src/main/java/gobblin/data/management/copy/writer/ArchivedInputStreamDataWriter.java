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

package gobblin.data.management.copy.writer;

import gobblin.configuration.State;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * An {@link InputStreamDataWriter} to write archived {@link InputStream}s. The {@link #write(FileAwareInputStream)}
 * method receives a {@link GZIPInputStream} and converts it to a {@link TarArchiveInputStream}. Each
 * {@link TarArchiveEntry} is then written to the {@link FileSystem}.
 */
@Slf4j
public class ArchivedInputStreamDataWriter extends InputStreamDataWriter {

  public ArchivedInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    super(state, numBranches, branchId);
  }

  /**
   * Untars the passed in {@link FileAwareInputStream} to the task's staging directory. Uses the name of the root
   * {@link TarArchiveEntry} in the stream as the directory name for the untarred file. The method also commits the data
   * by moving the file from staging to output directory.
   *
   * @param fileAwareInputStream the inputStream to be written. The {@link CopyableFile} instance enclosed is used to
   *          derive output file permissions
   * @see #commit()
   *
   * @see gobblin.data.management.copy.writer.InputStreamDataWriter#write(gobblin.data.management.copy.FileAwareInputStream)
   */
  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    closer.register(fileAwareInputStream.getInputStream());

    filesWritten++;

    TarArchiveInputStream tarIn = new TarArchiveInputStream(fileAwareInputStream.getInputStream());
    TarArchiveEntry tarEntry;

    boolean firstEntrySeen = false;

    try {
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {

        Path tarEntryRelativePath =
            new Path(fileAwareInputStream.getFile().getRelativeDestination().getParent(), tarEntry.getName());
        Path tarEntryStagingPath = new Path(this.stagingDir, tarEntryRelativePath);

        /*
         * If this is the first entry in the tar, reset the destination file name to the name of the first entry in the
         * tar. It is required to change the name of the destination file because the untarred file/dir name is known to
         * the source
         */
        if (!firstEntrySeen) {
          String newFileName = tarEntryStagingPath.getName();
          fileAwareInputStream.getFile().setDestination(
              new Path(fileAwareInputStream.getFile().getDestination().getParent(), newFileName));
          fileAwareInputStream.getFile().setRelativeDestination(
              new Path(fileAwareInputStream.getFile().getRelativeDestination().getParent(), newFileName));
          firstEntrySeen = true;
        }

        log.info("Unarchiving " + tarEntryStagingPath);

        if (tarEntry.isDirectory() && !fs.exists(tarEntryStagingPath)) {
          fs.mkdirs(tarEntryStagingPath);
        } else {

          FSDataOutputStream out = fs.create(tarEntryStagingPath, true);
          byte[] btoRead = new byte[1024];
          try {

            int len = 0;

            while ((len = tarIn.read(btoRead)) != -1) {
              bytesWritten += len;
              out.write(btoRead, 0, len);
            }

          } finally {
            out.close();
            btoRead = null;
          }
        }
      }
    } finally {
      tarIn.close();
    }

    // TODO add support for setting permission before commit.

    this.commit(fileAwareInputStream.getFile());
  }

}
