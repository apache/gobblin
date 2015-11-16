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
import gobblin.data.management.util.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


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
   * @param fileAwareInputStream the inputStream to be written. The {@link CopyableFile} instance enclosed is used to
   *          derive output file permissions
   * @see #commit()
   *
   * @see gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter#write(gobblin.data.management.copy.FileAwareInputStream)
   */
  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    closer.register(fileAwareInputStream.getInputStream());

    filesWritten++;

    TarArchiveInputStream tarIn = new TarArchiveInputStream(fileAwareInputStream.getInputStream());
    TarArchiveEntry tarEntry;

    Path fileDestinationPath = fileAwareInputStream.getFile().getDestination();

    // root entry in the tar compressed file
    String tarEntryRootName = null;

    try {
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {

        if (tarEntryRootName == null) {
          tarEntryRootName = StringUtils.remove(tarEntry.getName(), Path.SEPARATOR);
        }

        // the API tarEntry.getName() is misleading, it is actually the path of the tarEntry in the tar file
        String newTarEntryPath = tarEntry.getName().replace(tarEntryRootName, fileDestinationPath.getName());
        Path tarEntryDestinationPath =
            PathUtils.withoutLeadingSeparator(new Path(fileDestinationPath.getParent(), newTarEntryPath));

        Path tarEntryStagingPath = new Path(this.stagingDir, tarEntryDestinationPath);

        log.info("Unarchiving at " + tarEntryStagingPath);

        if (tarEntry.isDirectory() && !fs.exists(tarEntryStagingPath)) {
          fs.mkdirs(tarEntryStagingPath);
        } else if (!tarEntry.isDirectory()) {
          FSDataOutputStream out = fs.create(tarEntryStagingPath, true);
          try {
            IOUtils.copyBytes(tarIn, out, fs.getConf(), false);
          } finally {
            out.close();
          }
        }
      }
    } finally {
      tarIn.close();
      fileAwareInputStream.getInputStream().close();
    }

    this.setFilePermissions(fileAwareInputStream.getFile());
  }
}
