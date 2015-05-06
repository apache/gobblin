/* (c) 2015 NerdWallet All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * An implementation of {@link DataWriter} does the work of setting the output/staging dir
 * and creating the FileSystem instance.
 *
 * @author akshay@nerdwallet.com
 */
public abstract class BaseDataWriter<D> implements DataWriter<D> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsDataWriter.class);

  protected final FileSystem fs;
  protected final Path stagingFile;
  protected final Path outputFile;
  protected final State properties;

  public BaseDataWriter(State properties, String fileName, int numBranches, int branchId) throws IOException {
    this.properties = properties;
    // initialize file system
    String uri = properties
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
                    ConfigurationKeys.LOCAL_FS_URI);
    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : properties.getPropertyNames()) {
      conf.set(key, properties.getProp(key));
    }
    this.fs = FileSystem.get(URI.create(uri), conf);

    // initialize staging/output dir
    this.stagingFile = new Path(WriterUtils.getWriterStagingDir(properties, numBranches, branchId), fileName);
    this.outputFile = new Path(WriterUtils.getWriterOutputDir(properties, numBranches, branchId), fileName);
    properties.setProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, this.outputFile.toString());

    // Deleting the staging file if it already exists, which can happen if the
    // task failed and the staging file didn't get cleaned up for some reason.
    // Deleting the staging file prevents the task retry from being blocked.
    if (this.fs.exists(this.stagingFile)) {
      LOG.warn(String.format("Task staging file %s already exists, deleting it", this.stagingFile));
      this.deletePath(this.stagingFile, false);
    }

    // Create the parent directory of the output file if it does not exist
    if (!this.fs.exists(this.outputFile.getParent())) {
      this.fs.mkdirs(this.outputFile.getParent());
    }
  }

  /**
   * A wrapper around this.fs.delete which throws IOException if this.fs.delete returns False.
   * @param f Path to be deleted
   * @param recursive
   * @throws IOException if the deletion fails
   */
  protected void deletePath(Path f, boolean recursive) throws IOException {
    if (!this.fs.delete(f, recursive)) {
      throw new IOException("Failed to delete: " + f);
    }
  }
}
