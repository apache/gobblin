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
package gobblin.source.extractor.extract.google;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.drive.Drive;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;

/**
 * File system helper for Google drive.
 */
public class GoogleDriveFsHelper implements TimestampAwareFileBasedHelper {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleDriveFsHelper.class);

  static final String BUFFER_SIZE_BYTE = GoogleCommonKeys.GOOGLE_SOURCE_PREFIX + "buffer_size_bytes";
  private final FileSystem fileSystem;
  private final Closer closer;
  private final Optional<Integer> bufferSizeByte;

  public GoogleDriveFsHelper(State state, Drive client) {
    this(state, client, Closer.create());
  }

  @VisibleForTesting
  GoogleDriveFsHelper(State state, Drive client, Closer closer) {
    this.closer = closer;
    this.fileSystem = this.closer.register(new GoogleDriveFileSystem(client));
    if (state.contains(BUFFER_SIZE_BYTE)) {
      this.bufferSizeByte = Optional.of(state.getPropAsInt(BUFFER_SIZE_BYTE));
    } else {
      this.bufferSizeByte = Optional.absent();
    }
  }

  @Override
  public long getFileSize(String fileId) throws FileBasedHelperException {
    Preconditions.checkNotNull(fileId, "fileId is required");
    Path p = new Path(fileId);
    try {
      FileStatus status = fileSystem.getFileStatus(p);
      return status.getLen();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to get metadata on " + fileId, e);
    }
  }

  @Override
  public void connect() throws FileBasedHelperException {
    //None of other methods would not work without connect(). To make it simple, cstr already made it connected
    //so that it does not need to worry about having initialized or not and deal with race condition on initializing.
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

  /**
   * List files under folder ID recursively. Folder won't be included in the result. If there's no files under folder ID, it returns empty list.
   * If folder ID is not defined, it will provide files under root directory.
   * {@inheritDoc}
   * @see gobblin.source.extractor.filebased.FileBasedHelper#ls(java.lang.String)
   */
  @Override
  public List<String> ls(String folderId) throws FileBasedHelperException {
    List<String> result = new ArrayList<>();
    if (StringUtils.isEmpty(folderId)) {
      folderId = "/";
    }

    Path p = new Path(folderId);
    FileStatus[] statusList = null;
    try {
      statusList = fileSystem.listStatus(p);
    } catch (FileNotFoundException e) {
      return result;
    } catch (IOException e) {
      throw new FileBasedHelperException("Falied to list status on path " + p + ", folderID: " + folderId, e);
    }

    for (FileStatus status : statusList) {
      if (status.isDirectory()) {
        result.addAll(ls(GoogleDriveFileSystem.toFileId(status.getPath())));
      } else {
        result.add(GoogleDriveFileSystem.toFileId(status.getPath()));
      }
    }
    return result;
  }

  @Override
  public InputStream getFileStream(String fileId) throws FileBasedHelperException {
    Preconditions.checkNotNull(fileId, "fileId is required");
    Path p = new Path(fileId);
    try {
      if (bufferSizeByte.isPresent()) {
        return fileSystem.open(p, bufferSizeByte.get());
      }
      return fileSystem.open(p);
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to open files stream on path: " + p + " , fileId: " + fileId, e);
    }
  }

  /**
   * Permanently delete the file from Google drive (skipping trash)
   * @param path
   * @throws IOException
   */
  public void deleteFile(String fileId) throws IOException {
    Preconditions.checkNotNull(fileId, "fileId is required");
    Path p = new Path(fileId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path: " + p + " , fileId: " + fileId);
    }
    fileSystem.delete(p, true);
  }

  @Override
  public long getFileMTime(String fileId) throws FileBasedHelperException {
    Preconditions.checkNotNull(fileId, "fileId is required");
    Path p = new Path(fileId);

    try {
      FileStatus status = fileSystem.getFileStatus(p);
      return status.getModificationTime();
    } catch (IOException e) {
      throw new FileBasedHelperException("Failed to retrieve getModificationTime on path: " + p + " , fileId: " + fileId, e);
    }
  }
}
