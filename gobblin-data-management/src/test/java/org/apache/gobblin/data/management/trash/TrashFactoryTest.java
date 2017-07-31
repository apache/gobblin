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
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TrashFactoryTest {

  @Test
  public void test() throws IOException {
    FileSystem fs = mock(FileSystem.class);

    Path homeDirectory = new Path("/home/directory");
    Path trashDirectory = new Path(homeDirectory, Trash.DEFAULT_TRASH_DIRECTORY);
    Path trashIdentifierFile = new Path(trashDirectory, Trash.TRASH_IDENTIFIER_FILE);

    when(fs.getHomeDirectory()).thenReturn(homeDirectory);
    when(fs.exists(trashDirectory)).thenReturn(true);
    when(fs.exists(trashIdentifierFile)).thenReturn(true);
    when(fs.listStatus(trashDirectory)).thenReturn(new FileStatus[]{});
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

    Properties properties;

    properties = getBaseProperties(trashDirectory);
    Assert.assertTrue(TrashFactory.createTrash(fs, properties) instanceof Trash);
    Assert.assertTrue(TrashFactory.createProxiedTrash(fs, properties) instanceof ProxiedTrash);

    properties = getBaseProperties(trashDirectory);
    properties.setProperty(TrashFactory.SIMULATE, Boolean.toString(true));
    Assert.assertTrue(TrashFactory.createTrash(fs, properties) instanceof MockTrash);
    Assert.assertTrue(TrashFactory.createProxiedTrash(fs, properties) instanceof MockTrash);

    properties = getBaseProperties(trashDirectory);
    properties.setProperty(TrashFactory.TRASH_TEST, Boolean.toString(true));
    Assert.assertTrue(TrashFactory.createTrash(fs, properties) instanceof TestTrash);
    Assert.assertTrue(TrashFactory.createProxiedTrash(fs, properties) instanceof TestTrash);

    properties = getBaseProperties(trashDirectory);
    properties.setProperty(TrashFactory.SKIP_TRASH, Boolean.toString(true));
    Assert.assertTrue(TrashFactory.createTrash(fs, properties) instanceof ImmediateDeletionTrash);
    Assert.assertTrue(TrashFactory.createProxiedTrash(fs, properties) instanceof ImmediateDeletionTrash);

  }

  private Properties getBaseProperties(Path trashLocation) {
    Properties properties = new Properties();
    properties.setProperty(Trash.TRASH_LOCATION_KEY, trashLocation.toString());
    return properties;
  }

}
