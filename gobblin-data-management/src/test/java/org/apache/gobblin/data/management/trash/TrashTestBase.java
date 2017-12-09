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

package org.apache.gobblin.data.management.trash;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;


public class TrashTestBase {

  public static final String TEST_TRASH_LOCATION = "test.trash.location";
  FileSystem fs;
  Trash trash;

  public TrashTestBase(Properties properties) throws IOException {
    this(properties, true, true, false);
  }

  public TrashTestBase(Properties properties, boolean trashExists, boolean trashIdentifierExists, boolean trashEmpty)
      throws IOException {
    this.fs = mock(FileSystem.class);

    Path homeDirectory = new Path("/home/directory");
    Path trashDirectory = new Path(homeDirectory, Trash.DEFAULT_TRASH_DIRECTORY);
    if(properties.containsKey(TEST_TRASH_LOCATION)) {
      trashDirectory = new Path(properties.getProperty(TEST_TRASH_LOCATION));
    }
    Path trashIdentifierFile = new Path(trashDirectory, Trash.TRASH_IDENTIFIER_FILE);

    when(fs.getHomeDirectory()).thenReturn(homeDirectory);
    when(fs.exists(trashDirectory)).thenReturn(trashExists);
    when(fs.exists(trashIdentifierFile)).thenReturn(trashIdentifierExists);
    if(trashEmpty) {
      when(fs.listStatus(trashDirectory)).thenReturn(new FileStatus[]{});
    } else {
      when(fs.listStatus(trashDirectory)).thenReturn(new FileStatus[]{new FileStatus(0, true, 0, 0, 0, new Path("file"))});
    }
    when(fs.isDirectory(trashDirectory)).thenReturn(true);

    when(fs.mkdirs(any(Path.class))).thenReturn(true);
    when(fs.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(true);
    when(fs.createNewFile(any(Path.class))).thenReturn(true);
    when(fs.makeQualified(any(Path.class))).thenAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });

    this.trash = new Trash(fs, properties);
  }
}
