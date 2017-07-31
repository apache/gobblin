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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.mutable.MutableInt;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.Drive.Files.*;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import static org.apache.gobblin.source.extractor.extract.google.GoogleDriveFileSystem.*;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;

@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleDriveFsHelperTest {
  private Drive client;
  private Files files;

  private void setUp() {
    client = mock(Drive.class);
    files = mock(Files.class);
    when(client.files()).thenReturn(files);
  }

  public void closeTest() throws IOException, FileBasedHelperException {
    State state = new State();
    setUp();

    GoogleDriveFsHelper fsHelper = new GoogleDriveFsHelper(state, client, Closer.create());

    Get getResult = mock(Get.class);
    InputStream is = mock(InputStream.class);
    when(client.files()).thenReturn(files);
    when(files.get(anyString())).thenReturn(getResult);
    when(getResult.executeMediaAsInputStream()).thenReturn(is);

    fsHelper.getFileStream("test");
    fsHelper.close();

    verify(is, times(1)).close();
  }

  public void deleteTest() throws IOException {
    State state = new State();
    setUp();

    GoogleDriveFsHelper fsHelper = new GoogleDriveFsHelper(state, client, Closer.create());
    Delete delete = mock(Delete.class);
    String fileId = "test_file_id";
    when(files.delete(fileId)).thenReturn(delete);

    fsHelper.deleteFile(fileId);

    verify(delete, times(1)).execute();
  }

  public void testPagination() throws IOException, FileBasedHelperException {
    State state = new State();
    state.appendToSetProp(GoogleDriveFileSystem.PAGE_SIZE, Integer.toString(1));

    GoogleDriveFsHelper fsHelper = new GoogleDriveFsHelper(state, client, Closer.create());
    List listRequest = mock(List.class);
    when(files.list()).thenReturn(listRequest);
    when(listRequest.setPageSize(anyInt())).thenReturn(listRequest);
    when(listRequest.setFields(anyString())).thenReturn(listRequest);
    when(listRequest.setQ(anyString())).thenReturn(listRequest);
    when(listRequest.setPageToken(anyString())).thenReturn(listRequest);

    int paginatedCalls = 5;
    final MutableInt i = new MutableInt(paginatedCalls);
    final File file = new File();
    file.setId("testId");
    file.setModifiedTime(new DateTime(System.currentTimeMillis()));

    when(listRequest.execute()).thenAnswer(new Answer<FileList>() {

      @Override
      public FileList answer(InvocationOnMock invocation) throws Throwable {
        FileList fileList = new FileList();
        fileList.setFiles(ImmutableList.of(file));
        if (i.intValue() > 0) {
          fileList.setNextPageToken("token");
          i.decrement();
        }
        return fileList;
      }
    });

    fsHelper.ls("test");

    int expectedCalls = 1 + paginatedCalls;
    verify(listRequest, times(expectedCalls)).execute();
  }

  public void testList() throws IOException, FileBasedHelperException {
    java.util.List<String> filesRoot = Lists.newArrayList("f0_1", "f0_2", "f0_3", "f0_4","f0_5");
    java.util.List<String> filesL1 = Lists.newArrayList("f1_1", "f1_2", "f1_3", "f1_4");
    java.util.List<String> filesL2 = Lists.newArrayList("f2_1", "f2_2");

    String folderL1 = "folderL1";
    String folderL2 = "folderL2";
    String fileName = "test";

    FileList rootFileList = createFileList(filesRoot, folderL1);
    FileList FileListL1 = createFileList(filesL1, folderL2);
    FileList FileListL2 = createFileList(filesL2, null);

    State state = new State();
    state.appendToSetProp(GoogleDriveFileSystem.PAGE_SIZE, Integer.toString(1));

    GoogleDriveFsHelper fsHelper = new GoogleDriveFsHelper(state, client, Closer.create());
    List listRequest = mock(List.class);
    when(files.list()).thenReturn(listRequest);
    when(listRequest.setFields(anyString())).thenReturn(listRequest);
    when(listRequest.setPageSize(anyInt())).thenReturn(listRequest);

    GoogleDriveFileSystem fs = new GoogleDriveFileSystem();
    when(listRequest.execute()).thenReturn(rootFileList);

    List ListL1Request = mock(List.class);
    when(listRequest.setQ(fs.buildQuery(folderL1, null).get())).thenReturn(ListL1Request);
    when(ListL1Request.execute()).thenReturn(FileListL1);

    List ListL2Request = mock(List.class);
    when(listRequest.setQ(fs.buildQuery(folderL2, null).get())).thenReturn(ListL2Request);
    when(ListL2Request.execute()).thenReturn(FileListL2);

    java.util.List<String> actual = fsHelper.ls(folderL2);
    java.util.List<String> expected = Lists.newArrayList(filesL2);
    Assert.assertTrue(actual.containsAll(expected) && expected.containsAll(actual));

    actual = fsHelper.ls(folderL1);
    expected.addAll(filesL1);
    Assert.assertTrue(actual.containsAll(expected) && expected.containsAll(actual));

    actual = fsHelper.ls(null);
    expected.addAll(filesRoot);
    Assert.assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
  }

  private FileList createFileList(java.util.List<String> fileIds, String folderId) {
    FileList fileList = new FileList();
    java.util.List<File> list = Lists.newArrayList();
    for (String fileId : fileIds) {
      File f = new File();
      f.setId(fileId);
      f.setModifiedTime(new DateTime(System.currentTimeMillis()));
      list.add(f);
    }

    if (folderId != null) {
      File f = new File();
      f.setMimeType(FOLDER_MIME_TYPE);
      f.setId(folderId);
      f.setModifiedTime(new DateTime(System.currentTimeMillis()));
      list.add(f);
    }
    fileList.setFiles(list);
    return fileList;
  }
}