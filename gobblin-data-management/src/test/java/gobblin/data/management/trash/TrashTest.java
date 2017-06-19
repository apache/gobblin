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

package gobblin.data.management.trash;


import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.*;

@Test(groups = { "SystemTimeTests"})
public class TrashTest {

  @Test
  public void testCreateTrash() throws IOException {

    new TrashTestBase(new Properties());

  }

  @Test
  public void testCreationCases() throws IOException {

    TrashTestBase trash;

    // If trash ident file doesn't exist, but trash is empty, create
    trash = new TrashTestBase(new Properties(), true, false, true);
    verify(trash.fs).createNewFile(new Path(trash.trash.getTrashLocation(), Trash.TRASH_IDENTIFIER_FILE));


    // If trash ident file doesn't exist, but trash is not empty, fail
    try {
      trash = new TrashTestBase(new Properties(), true, false, false);
      Assert.fail();
    } catch(IOException ioe) {}

    trash = new TrashTestBase(new Properties(), false, false, true);
    verify(trash.fs).mkdirs(trash.trash.getTrashLocation(), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    verify(trash.fs).createNewFile(new Path(trash.trash.getTrashLocation(), Trash.TRASH_IDENTIFIER_FILE));
  }

  @Test
  public void testUserReplacement() throws IOException {

    Properties properties = new Properties();
    properties.setProperty(Trash.TRASH_LOCATION_KEY, "/trash/$USER/dir");
    Path expectedTrashPath = new Path("/trash/" + UserGroupInformation.getCurrentUser().getUserName() + "/dir");

    TrashTestBase trash = new TrashTestBase(properties);

    Assert.assertTrue(trash.trash.getTrashLocation().equals(expectedTrashPath));
  }

  @Test
  public void testMoveToTrash() throws IOException {

    TrashTestBase trash = new TrashTestBase(new Properties());

    Path pathToDelete = new Path("/path/to/delete");

    final List<Pair<Path, Path>> movedPaths = Lists.newArrayList();

    when(trash.fs.exists(any(Path.class))).thenReturn(false);
    when(trash.fs.rename(any(Path.class), any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        movedPaths.add(new Pair<Path, Path>((Path) args[0], (Path) args[1]));
        return true;
      }
    });

    Assert.assertTrue(trash.trash.moveToTrash(pathToDelete));

    verify(trash.fs, times(1)).mkdirs(any(Path.class));

    Assert.assertEquals(movedPaths.size(), 1);
    Assert.assertTrue(movedPaths.get(0).first().equals(pathToDelete));
    Assert.assertTrue(movedPaths.get(0).second().toString().endsWith(pathToDelete.toString()));
    Assert.assertTrue(movedPaths.get(0).second().getParent().getParent().getParent().equals(trash.trash.getTrashLocation()));

  }

  @Test
  public void testMoveToTrashExistingFile() throws IOException {

    TrashTestBase trash = new TrashTestBase(new Properties());

    String fileName = "delete";

    Path pathToDelete = new Path("/path/to", fileName);
    Pattern expectedNamePattern = Pattern.compile("^" + fileName + "_[0-9]+$");

    final List<Pair<Path, Path>> movedPaths = Lists.newArrayList();

    when(trash.fs.exists(any(Path.class))).thenReturn(true);
    when(trash.fs.rename(any(Path.class), any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        movedPaths.add(new Pair<Path, Path>((Path) args[0], (Path) args[1]));
        return true;
      }
    });

    Assert.assertTrue(trash.trash.moveToTrash(pathToDelete));

    verify(trash.fs, times(0)).mkdirs(any(Path.class));

    Assert.assertEquals(movedPaths.size(), 1);
    Assert.assertTrue(movedPaths.get(0).first().equals(pathToDelete));
    Assert.assertTrue(movedPaths.get(0).second().getParent().toString().endsWith(pathToDelete.getParent().toString()));
    Assert.assertTrue(expectedNamePattern.matcher(movedPaths.get(0).second().getName()).matches());
    Assert.assertTrue(movedPaths.get(0).second().getParent().getParent().getParent().equals(trash.trash.getTrashLocation()));

  }

  @Test
  public void testCreateSnapshot() throws IOException {

    try {
      TrashTestBase trash = new TrashTestBase(new Properties());

      Path pathInTrash = new Path(trash.trash.getTrashLocation(), "dirInTrash");

      DateTimeUtils.setCurrentMillisFixed(new DateTime(2015, 7, 15, 10, 0).getMillis());

      final List<Path> createdDirs = Lists.newArrayList();
      final List<Pair<Path, Path>> movedPaths = Lists.newArrayList();

      when(trash.fs.listStatus(eq(trash.trash.getTrashLocation()), any(PathFilter.class))).
          thenReturn(Lists.newArrayList(new FileStatus(0, true, 0, 0, 0, pathInTrash)).toArray(new FileStatus[]{}));
      when(trash.fs.exists(any(Path.class))).thenReturn(false);
      when(trash.fs.mkdirs(any(Path.class), any(FsPermission.class))).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation)
            throws Throwable {
          createdDirs.add((Path) invocation.getArguments()[0]);
          return true;
        }
      });
      when(trash.fs.rename(any(Path.class), any(Path.class))).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation)
            throws Throwable {
          Object[] args = invocation.getArguments();
          movedPaths.add(new Pair<Path, Path>((Path) args[0], (Path) args[1]));
          return true;
        }
      });

      trash.trash.createTrashSnapshot();

      Assert.assertEquals(createdDirs.size(), 1);
      Path createdDir = createdDirs.get(0);
      Assert.assertTrue(Trash.TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(createdDir.getName()).equals(new DateTime().withZone(
          DateTimeZone.UTC)));
      Assert.assertEquals(movedPaths.size(), 1);
      Assert.assertTrue(movedPaths.get(0).first().equals(pathInTrash));
      Assert.assertTrue(movedPaths.get(0).second().getName().equals(pathInTrash.getName()));
      Assert.assertTrue(movedPaths.get(0).second().getParent().equals(createdDir));
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

  @Test
  public void testPurgeSnapshots() throws IOException {

    try {
      Properties properties = new Properties();
      properties.setProperty(Trash.SNAPSHOT_CLEANUP_POLICY_CLASS_KEY, TestCleanupPolicy.class.getCanonicalName());

      TrashTestBase trash = new TrashTestBase(properties);

      DateTimeUtils.setCurrentMillisFixed(new DateTime(2015, 7, 15, 10, 0).withZone(DateTimeZone.UTC).getMillis());

      final List<Path> deletedPaths = Lists.newArrayList();

      Path snapshot1 = new Path(trash.trash.getTrashLocation(), Trash.TRASH_SNAPSHOT_NAME_FORMATTER.print(new DateTime()));
      Path snapshot2 = new Path(trash.trash.getTrashLocation(),
          Trash.TRASH_SNAPSHOT_NAME_FORMATTER.print(new DateTime().minusDays(1)));

      when(trash.fs.listStatus(any(Path.class), any(PathFilter.class))).
          thenReturn(
              Lists.newArrayList(
                  new FileStatus(0, true, 0, 0, 0, snapshot1),
                  new FileStatus(0, true, 0, 0, 0, snapshot2))
                  .toArray(new FileStatus[]{}));
      when(trash.fs.delete(any(Path.class), anyBoolean())).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation)
            throws Throwable {
          deletedPaths.add((Path) invocation.getArguments()[0]);
          return true;
        }
      });

      trash.trash.purgeTrashSnapshots();

      Assert.assertEquals(deletedPaths.size(), 1);
      Assert.assertTrue(deletedPaths.get(0).equals(snapshot2));
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

}
