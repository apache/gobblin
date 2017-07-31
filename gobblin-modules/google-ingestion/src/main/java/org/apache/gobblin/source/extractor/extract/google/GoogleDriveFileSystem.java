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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;
import static org.apache.gobblin.source.extractor.extract.google.GoogleCommonKeys.*;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.io.SeekableFSInputStream;

/**
 * A {@link FileSystem} implementation that provides the {@link FileSystem} interface for an Google Drive server.
 * <ul>
 * <li>Note that {@link GoogleDriveFileSystem} currently only supports list, get, delete use cases.
 * <li>Google drive has two different identifier. File ID and File name -- where folder is just different mime-type of File.
 * As File name can be duplicate under same folder, all path that GoogleDriveFileSystem takes assumes that it's a File ID.
 * <li>It is the caller's responsibility to call {@link #close()} on this {@link FileSystem} to disconnect the session.
 *
 * {@link GoogleDriveFileSystem} does not cache instance and {@link FileSystem#get(Configuration)} creates a new {@link GoogleDriveFileSystem} everytime
 * instead of cached copy.
 * </ul>
 */
public class GoogleDriveFileSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleDriveFileSystem.class);

  static final String PAGE_SIZE = GOOGLE_SOURCE_PREFIX + "fs_helper.page_size"; //for paginated API
  static final String FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
  static final int DEFAULT_PAGE_SIZE = 50;

  private Drive client;
  private final Closer closer;
  private int pageSize = DEFAULT_PAGE_SIZE;

  public GoogleDriveFileSystem(Drive client) {
    this();
    this.client = client;
  }

  public GoogleDriveFileSystem(Drive client, int pageSize) {
    this(client);
    Preconditions.checkArgument(pageSize > 0, "pageSize should be positive number");
    this.pageSize = pageSize;
  }

  public GoogleDriveFileSystem() {
    super();
    this.closer = Closer.create();
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf) throws IOException {
    if (this.client == null) {
      super.initialize(uri, conf);
      State state = HadoopUtils.getStateFromConf(conf);
      Credential credential = new GoogleCommon.CredentialBuilder(state.getProp(SOURCE_CONN_PRIVATE_KEY), state.getPropAsList(API_SCOPES))
                                              .fileSystemUri(state.getProp(PRIVATE_KEY_FILESYSTEM_URI))
                                              .proxyUrl(state.getProp(SOURCE_CONN_USE_PROXY_URL))
                                              .port(state.getProp(SOURCE_CONN_USE_PROXY_PORT))
                                              .serviceAccountId(state.getProp(SOURCE_CONN_USERNAME))
                                              .build();

      this.client = new Drive.Builder(credential.getTransport(),
                                      GoogleCommon.getJsonFactory(),
                                      credential)
                             .setApplicationName(Preconditions.checkNotNull(state.getProp(APPLICATION_NAME),
                                       "ApplicationName is required"))
                             .build();
      this.pageSize = state.getPropAsInt(PAGE_SIZE, DEFAULT_PAGE_SIZE);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return closer.register(new FSDataInputStream(
        new SeekableFSInputStream(
            new BufferedInputStream(
                client.files().get(toFileId(path)).executeMediaAsInputStream(), bufferSize))));
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return closer.register(new FSDataInputStream(
        new SeekableFSInputStream(
            new BufferedInputStream(
                client.files().get(toFileId(path)).executeMediaAsInputStream()))));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    Preconditions.checkArgument(recursive, "Non-recursive is not supported.");
    String fileId = toFileId(path);
    LOG.debug("Deleting file: " + fileId);
    try {
      client.files().delete(fileId).execute();
    } catch (GoogleJsonResponseException e) {
      GoogleJsonError error = e.getDetails();
      if (404 == error.getCode()) { //Non-existing file id
        return false;
      }
      throw e;
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    String folderId = toFileId(path);

    List<File> fileMetadata = lsFileMetadata(folderId, null);
    if (fileMetadata.isEmpty()) {
      throw new FileNotFoundException();
    }
    FileStatus[] statusArr = new FileStatus[fileMetadata.size()];
    int idx = 0;
    for (File metadata: fileMetadata) {
      FileStatus status = toFileStatus(metadata);
      statusArr[idx++] = status;
    }
    return statusArr;
  }

  private List<File> lsFileMetadata(String folderId, String fileId) throws IOException {
    String pageToken = null;
    List<File> result = new ArrayList<>();
    Optional<String> query = buildQuery(folderId, fileId);

    do {
      Drive.Files.List request = client.files()
          .list()
          .setFields("files/id,files/mimeType,files/modifiedTime,files/size,files/permissions")
          .setPageSize(pageSize);
      if (query.isPresent()) {
        request = request.setQ(query.get());
      }

      if (pageToken != null) {
        request = request.setPageToken(pageToken);
      }

      LOG.info("Google drive List request: " + request);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Google drive List request: " + request);
      }

      FileList fileList = null;
      try {
        fileList = request.execute();
      } catch (GoogleJsonResponseException e) {
        GoogleJsonError error = e.getDetails();
        if (404 == error.getCode()) {
          throw new FileNotFoundException("File not found. Request: " + request);
        }
        throw e;
      }

      pageToken = fileList.getNextPageToken();

      List<File> files = fileList.getFiles();
      if (files == null || files.isEmpty()) {
        return result;
      }
      result.addAll(files);
    } while (pageToken != null);
    return result;
  }

  /**
   * Build query for Google drive.
   * @see https://developers.google.com/drive/v3/web/search-parameters
   *
   * @param folderId
   * @param fileName
   * @return Query
   */
  @VisibleForTesting
  Optional<String> buildQuery(String folderId, String fileName) {
    if (StringUtils.isEmpty(folderId) && StringUtils.isEmpty(fileName)) {
      return Optional.absent();
    }

    StringBuilder query = new StringBuilder();
    if (StringUtils.isNotEmpty(folderId)) {
      query.append("'").append(folderId).append("'")
           .append(" in parents");
    }
    if (StringUtils.isNotEmpty(fileName)) {
      if (query.length() > 0) {
        query.append(" and ");
      }
      query.append("name contains ")
           .append("'").append(fileName).append("'");
    }
    return Optional.of(query.toString());
  }

  /**
   * org.apache.hadoop.fs.Path assumes that there separator in file system naming and "/" is the separator.
   * When org.apache.hadoop.fs.Path sees "/" in path String, it splits into parent and name. As fileID is a random
   * String determined by Google and it can contain "/" itself, this method check if parent and name is separated and
   * restore "/" back to file ID.
   *
   * @param p
   * @return
   */
  public static String toFileId(Path p) {
    if (p.isRoot()) {
      return "";
    }
    final String format = "%s" + Path.SEPARATOR + "%s";
    if (p.getParent() != null && StringUtils.isEmpty(p.getParent().getName())) {
      return p.getName();
    }
    return String.format(format, toFileId(p.getParent()), p.getName());
  }

  @Override
  public void close() throws IOException {
    super.close();
    closer.close();
  }


  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    Preconditions.checkNotNull(p);
    String fileId = toFileId(p);
    File metadata = client.files().get(fileId)
                                  .setFields("id,mimeType,modifiedTime,size,permissions")
                                  .execute();
    return toFileStatus(metadata);
  }

  private FileStatus toFileStatus(File metadata) {
    return new FileStatus(metadata.getSize() == null ? 0L : metadata.getSize(),
                          FOLDER_MIME_TYPE.equals(metadata.getMimeType()),
                          -1,
                          -1,
                          metadata.getModifiedTime().getValue(),
                          new Path(metadata.getId()));
  }

  //Below are unsupported methods

  @Override
  public void setWorkingDirectory(Path new_dir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException();
  }


  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException();
  }
}
