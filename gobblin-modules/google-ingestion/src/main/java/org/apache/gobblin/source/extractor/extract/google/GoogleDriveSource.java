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
package org.apache.gobblin.source.extractor.extract.google;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.drive.Drive;
import com.google.common.io.Closer;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;
import static org.apache.gobblin.source.extractor.extract.google.GoogleCommonKeys.*;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.FileBasedSource;

/**
 * Source for Google drive using GoogleDriveFsHelper.
 * @param <S>
 * @param <D>
 */
public class GoogleDriveSource<S, D> extends FileBasedSource<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleDriveSource.class);
  public static final String GOOGLE_DRIVE_PREFIX = GOOGLE_SOURCE_PREFIX + "drive.";
  public static final String BUFFER_BYTE_SIZE = "buffer_byte_size";

  private final Closer closer = Closer.create();
  /**
   * As Google Drive extractor needs file system helper, it invokes to initialize file system helper.
   * {@inheritDoc}
   * @see org.apache.gobblin.source.Source#getExtractor(org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    Preconditions.checkNotNull(state, "WorkUnitState should not be null");
    LOG.info("WorkUnitState from getExtractor: " + state);

    try {
      //GoogleDriveExtractor needs GoogleDriveFsHelper
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      throw new IOException(e);
    }

    Preconditions.checkNotNull(fsHelper, "File system helper should not be null");
    return new GoogleDriveExtractor<>(state, fsHelper);
  }

  /**
   * Initialize file system helper at most once for this instance.
   * {@inheritDoc}
   * @see org.apache.gobblin.source.extractor.filebased.FileBasedSource#initFileSystemHelper(org.apache.gobblin.configuration.State)
   */
  @Override
  public synchronized void initFileSystemHelper(State state) throws FileBasedHelperException {
    if (fsHelper == null) {
      Credential credential = new GoogleCommon.CredentialBuilder(state.getProp(SOURCE_CONN_PRIVATE_KEY), state.getPropAsList(API_SCOPES))
                                              .fileSystemUri(state.getProp(PRIVATE_KEY_FILESYSTEM_URI))
                                              .proxyUrl(state.getProp(SOURCE_CONN_USE_PROXY_URL))
                                              .port(state.getProp(SOURCE_CONN_USE_PROXY_PORT))
                                              .serviceAccountId(state.getProp(SOURCE_CONN_USERNAME))
                                              .build();

      Drive driveClient = new Drive.Builder(credential.getTransport(),
                                            GoogleCommon.getJsonFactory(),
                                            credential)
                                   .setApplicationName(Preconditions.checkNotNull(state.getProp(APPLICATION_NAME), "ApplicationName is required"))
                                   .build();
      this.fsHelper = closer.register(new GoogleDriveFsHelper(state, driveClient));
    }
  }

  /**
   * Provide list of files snapshot where snap shot is consist of list of file ID with modified time.
   * Folder ID and file ID are all optional where missing folder id represent search from root folder where
   * missing file ID represents all files will be included on current and subfolder.
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.source.extractor.filebased.FileBasedSource#getcurrentFsSnapshot(org.apache.gobblin.configuration.State)
   */
  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = new ArrayList<>();

    String folderId = state.getProp(SOURCE_FILEBASED_DATA_DIRECTORY, "");

    try {
      LOG.info("Running ls with folderId: " + folderId);
      List<String> fileIds = this.fsHelper.ls(folderId);
      for (String fileId : fileIds) {
        results.add(fileId + splitPattern + this.fsHelper.getFileMTime(fileId));
      }
    } catch (FileBasedHelperException e) {
      throw new RuntimeException("Failed to retrieve list of file IDs for folderID: " + folderId, e);
    }
    return results;
  }

  @Override
  public void shutdown(SourceState state) {
    try {
      closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
