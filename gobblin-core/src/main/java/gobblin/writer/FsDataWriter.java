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

package gobblin.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.ProxiedFileSystemWrapper;
import gobblin.util.WriterUtils;


/**
 * An implementation of {@link DataWriter} does the work of setting the output/staging dir
 * and creating the FileSystem instance.
 *
 * @author akshay@nerdwallet.com
 */
public abstract class FsDataWriter<D> implements DataWriter<D>, FinalState {
  private static final Logger LOG = LoggerFactory.getLogger(FsDataWriter.class);

  protected final FileSystem fs;
  protected final Path stagingFile;
  protected final Path outputFile;
  protected final State properties;

  public FsDataWriter(State properties, String fileName, int numBranches, int branchId) throws IOException {
    this.properties = properties;

    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    JobConfigurationUtils.putStateIntoConfiguration(properties, conf);

    String uri =
        properties
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
                branchId), ConfigurationKeys.LOCAL_FS_URI);

    if (properties.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system as a proxy user.
      try {
        this.fs =
            new ProxiedFileSystemWrapper().getProxiedFileSystem(properties, ProxiedFileSystemWrapper.AuthType.TOKEN,
                properties.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE), uri);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    } else {
      // Initialize file system as the current user.
      this.fs = FileSystem.get(URI.create(uri), conf);
    }

    // initialize staging/output dir
    this.stagingFile = new Path(WriterUtils.getWriterStagingDir(properties, numBranches, branchId), fileName);
    this.outputFile = new Path(WriterUtils.getWriterOutputDir(properties, numBranches, branchId), fileName);
    this.properties.setProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, branchId),
        this.outputFile.toString());

    // Deleting the staging file if it already exists, which can happen if the
    // task failed and the staging file didn't get cleaned up for some reason.
    // Deleting the staging file prevents the task retry from being blocked.
    if (this.fs.exists(this.stagingFile)) {
      LOG.warn(String.format("Task staging file %s already exists, deleting it", this.stagingFile));
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }

    // Create the parent directory of the output file if it does not exist
    if (!this.fs.exists(this.outputFile.getParent())) {
      this.fs.mkdirs(this.outputFile.getParent());
    }
  }

  @Override
  public State getFinalState() {
    State state = new State();
    state.setProp("RecordsWritten", recordsWritten());
    try {
      state.setProp("BytesWritten", bytesWritten());
    } catch(IOException ioe) {
    }
    return state;
  }
}
