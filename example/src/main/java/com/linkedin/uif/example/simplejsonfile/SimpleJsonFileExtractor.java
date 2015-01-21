/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.example.simplejsonfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;


/**
 * A demo implementation of {@link Extractor}.
 *
 * <p>
 *   This extractor uses the commons-vfs library to read the assigned input file storing
 *   json documents confirming to a schema. Each line of the file is a json document.
 * </p>
 *
 * @author ynli
 */
public class SimpleJsonFileExtractor implements Extractor<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJsonFileExtractor.class);

  private static final String SOURCE_FILE_KEY = "source.file";

  private final WorkUnitState workUnitState;
  private final FileObject fileObject;
  private final BufferedReader bufferedReader;

  public SimpleJsonFileExtractor(WorkUnitState workUnitState)
      throws FileSystemException {
    this.workUnitState = workUnitState;

    // Resolve the file to pull
    if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_CONN_USE_AUTHENTICATION, false)) {
      // Add authentication credential if authentication is needed
      UserAuthenticator auth =
          new StaticUserAuthenticator(workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_DOMAIN, ""),
              workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME),
              workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
      FileSystemOptions opts = new FileSystemOptions();
      DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
      this.fileObject = VFS.getManager().resolveFile(workUnitState.getProp(SOURCE_FILE_KEY), opts);
    } else {
      this.fileObject = VFS.getManager().resolveFile(workUnitState.getProp(SOURCE_FILE_KEY));
    }

    // Open the file for reading
    LOGGER.info("Opening file " + this.fileObject.getURL().toString());
    this.bufferedReader = new BufferedReader(new InputStreamReader(this.fileObject.getContent().getInputStream()));
  }

  @Override
  public String getSchema() {
    return this.workUnitState.getProp(ConfigurationKeys.SOURCE_SCHEMA);
  }

  @Override
  public String readRecord(String reuse)
      throws DataRecordException, IOException {
    // Read the next line
    return this.bufferedReader.readLine();
  }

  @Override
  public long getExpectedRecordCount() {
    // We don't know how many records are in the file before actually reading them
    return 0;
  }

  @Override
  public long getHighWatermark() {
    // Watermark is not applicable for this type of extractor
    return 0;
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.bufferedReader.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close the input stream", ioe);
    }

    try {
      this.fileObject.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close the file object", ioe);
    }
  }
}
