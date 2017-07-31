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

package org.apache.gobblin.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import org.testng.Assert;
import org.testng.annotations.Test;


public class MetricsFileSystemInstrumentationTest {

  private final String originalURI = "hdfs://localhost:9000";
  private final String instrumentedURI = "instrumented-hdfs://localhost:9000";

  public class HDFSRoot {
    @Getter
    private FileSystem fs;
    @Getter
    private Path rootPath;
    @Getter
    private Path filePath1, filePath2, filePath3, filePath4, filePath5, filePath6, filePath7, filePath8;
    @Getter
    private Path dirPath1, dirPath2, dirPath3, dirPath4, dirPath5, dirPath6;

    // /tmp -> root -> file1
    //              -> file2
    //              -> dir1
    //              -> dir2.ext -> file3
    //                          -> file4
    //              -> dir3     -> file5.ext
    //                          -> file6.ext
    //                          -> dir4      -> dir5
    //                                       -> dir6 -> file7.ext
    //              -> file8.ext

    public HDFSRoot(String root) throws IOException, URISyntaxException {

      this.fs = FileSystem.get(new URI(originalURI), new Configuration());
      this.rootPath = new Path(root);
      fs.delete(rootPath, true);
      fs.mkdirs(rootPath);

      // Test absolute paths
      String file1 = "file1";
      filePath1 = new Path(root, file1);
      fs.createNewFile(filePath1);

      String file2 = "file2";
      filePath2 = new Path(root, file2);
      fs.createNewFile(filePath2);

      String dir1 = "dir1";
      dirPath1 = new Path(root, dir1);
      fs.mkdirs(dirPath1);

      String dir2 = "dir2";
      dirPath2 = new Path(root, dir2 + ".ext");
      fs.mkdirs(dirPath2);

      String dir3 = "dir3";
      dirPath3 = new Path(root, dir3);
      fs.mkdirs(dirPath3);

      String file3 = "file3";
      filePath3 = new Path(dirPath2, file3);
      fs.createNewFile(filePath3);

      String file4 = "file4";
      filePath4 = new Path(dirPath2, file4);
      fs.createNewFile(filePath4);

      String file5 = "file5";
      filePath5 = new Path(dirPath3, file5 + ".ext");
      fs.createNewFile(filePath5);

      String file6 = "file6";
      filePath6 = new Path(dirPath3, file6 + ".ext");
      fs.createNewFile(filePath6);

      String dir4 = "dir4";
      dirPath4 = new Path(dirPath3, dir4);
      fs.mkdirs(dirPath4);

      String dir5 = "dir5";
      dirPath5 = new Path(dirPath4, dir5);
      fs.mkdirs(dirPath5);

      String dir6 = "dir6";
      dirPath6 = new Path(dirPath4, dir6);
      fs.mkdirs(dirPath6);

      String file7 = "file7";
      filePath7 = new Path(dirPath6, file7 + ".ext");
      fs.createNewFile(filePath7);

      String file8 = "file8";
      filePath8 = new Path(root, file8 + ".ext");
      fs.createNewFile(filePath8);
    }

    public void cleanupRoot() throws IOException {
      fs.delete(rootPath, true);
    }
  }

  /**
   * This test is disabled because it requires a local hdfs cluster at localhost:8020, which requires installation and setup.
   * Changes to {@link MetricsFileSystemInstrumentation} should be followed by a manual run of this tests.
   *
   * TODO: figure out how to fully automate this test.
   * @throws Exception
   */
  @Test(enabled = false)
  public void test() throws Exception {

    String uri = "instrumented-hdfs://localhost:9000";

    FileSystem fs = FileSystem.get(new URI(uri), new Configuration());

    String name = UUID.randomUUID().toString();
    fs.mkdirs(new Path("/tmp"));

    // Test absolute paths
    Path absolutePath = new Path("/tmp", name);
    Assert.assertFalse(fs.exists(absolutePath));
    fs.createNewFile(absolutePath);
    Assert.assertTrue(fs.exists(absolutePath));
    Assert.assertEquals(fs.getFileStatus(absolutePath).getLen(), 0);
    fs.delete(absolutePath, false);
    Assert.assertFalse(fs.exists(absolutePath));


    // Test fully qualified paths
    Path fqPath = new Path(uri + "/tmp", name);
    Assert.assertFalse(fs.exists(fqPath));
    fs.createNewFile(fqPath);
    Assert.assertTrue(fs.exists(fqPath));
    Assert.assertEquals(fs.getFileStatus(fqPath).getLen(), 0);
    fs.delete(fqPath, false);
    Assert.assertFalse(fs.exists(fqPath));
  }

  @Test(enabled = false)
  public void testListStatusPath() throws IOException, URISyntaxException  {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPath");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path rootPath = hdfsRoot.getRootPath();
    FileStatus[] status = fs.listStatus(rootPath);
    Assert.assertEquals(fs.listStatusTimer.getCount(), 1);
    Assert.assertEquals(status.length, 6);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testListStatusPathError() throws IOException, URISyntaxException {

    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPathError");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    try {
      fs.listStatus(new Path("/tmp/nonexistence"));
    } catch (Exception e) {
      // stop search when a non-existed directory was encountered, the visit of non-existed path is sill considered as one visit.
      Assert.assertEquals(fs.listStatusTimer.getCount(), 1);
    } finally {
      hdfsRoot.cleanupRoot();
    }
  }

  @Test(enabled = false)
  public void testListStatusPaths() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPaths");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    Path[] paths = {hdfsRoot.filePath2, hdfsRoot.dirPath2};
    FileStatus[] status = fs.listStatus(paths);

    Assert.assertEquals(fs.listStatusTimer.getCount(), 2);
    Assert.assertEquals(status.length, 3);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testListStatusPathsError() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPathsError");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    try {
      Path[] paths = {hdfsRoot.dirPath2, new Path("/tmp/nonexistence"), hdfsRoot.filePath2};
      fs.listStatus(paths);
    } catch (Exception e) {
      // stop search when a non-existed directory was encountered, the visit of non-existed path is sill considered as one visit.
      Assert.assertEquals(fs.listStatusTimer.getCount(), 2);
    } finally {
      hdfsRoot.cleanupRoot();
    }
  }

  @Test(enabled = false)
  public void testListStatusPathWithFilter() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPathWithFilter");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FileStatus[] status = fs.listStatus(hdfsRoot.getDirPath3(), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().endsWith(".ext");
      }
    });
    Assert.assertEquals(fs.listStatusTimer.getCount(), 1);
    Assert.assertEquals(status.length, 2);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testListStatusPathsWithFilter() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListStatusPathsWithFilter");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    Path[] paths = {hdfsRoot.filePath2, hdfsRoot.dirPath2, hdfsRoot.dirPath3};
    FileStatus[] status = fs.listStatus(paths, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().endsWith(".ext");
      }
    });

    Assert.assertEquals(fs.listStatusTimer.getCount(), 3);
    Assert.assertEquals(status.length, 2);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testListFiles() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/ListFiles");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    fs.listFiles(hdfsRoot.getRootPath(), true);
    Assert.assertEquals(fs.listFilesTimer.getCount(), 1);
    Assert.assertEquals(fs.listStatusTimer.getCount(), 0);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testGlobStatus() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/GlobStatus");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    FileStatus[] status = fs.globStatus(new Path("/tmp/GlobStatus/*/*.ext"));
    Assert.assertEquals(fs.globStatusTimer.getCount(), 1);
    Assert.assertEquals(status.length, 2);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testGlobStatusWithFilter() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/GlobStatusWithFilter");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    FileStatus[] status = fs.globStatus(new Path("/tmp/GlobStatusWithFilter/*/*"), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().endsWith(".ext");
      }
    });
    Assert.assertEquals(fs.globStatusTimer.getCount(), 1);
    Assert.assertEquals(status.length, 2);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testMakeDirWithPermission() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/mkdirWithPermission");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    Path newDir = new Path (hdfsRoot.getRootPath(), new Path("X"));
    fs.mkdirs(newDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ));
    Assert.assertEquals(fs.mkdirTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testMakeDirs() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/mkdirs");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    Path newDir = new Path (hdfsRoot.getRootPath(), new Path("X/Y/Z"));
    fs.mkdirs(newDir);
    Assert.assertEquals(fs.mkdirTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testMakeDirsWithPermission() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/mkdirsWithPermission");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    Path newDir = new Path (hdfsRoot.getRootPath(), new Path("X/Y/Z"));
    fs.mkdirs(newDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ));
    Assert.assertEquals(fs.mkdirTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testDelete() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/delete");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());

    fs.delete(hdfsRoot.getDirPath3(), true);
    Assert.assertEquals(fs.deleteTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testRename() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/rename");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newDir = new Path("/tmp/rename/AfterRename");
    fs.rename(hdfsRoot.getDirPath3(), newDir);
    Assert.assertEquals(fs.renameTimer.getCount(), 1);

    Assert.assertFalse(fs.exists(hdfsRoot.getDirPath3()));
    Assert.assertTrue(fs.exists(newDir));
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate1() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate2() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, true);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate3() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, true, 300);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate4() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, true, 300, null);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate5() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, true, 300, (short)1, 1048576);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate6() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, true, 300, (short)1, 1048576, null);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate7() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ);
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, permission, true, 100, (short)2, 1048576, null);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate8() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, (short)2);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate9() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, (short)2, null);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testCreate10() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/create");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path("/tmp/create/newFile");
    FSDataOutputStream fstream = fs.create(newFile, null);
    Assert.assertEquals(fs.createTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testOpen1() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/Open");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path(hdfsRoot.getRootPath(), new Path("file8.ext"));
    FSDataInputStream fstream = fs.open(newFile);
    Assert.assertEquals(fs.openTimer.getCount(), 1);

    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testOpen2() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/Open");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path(hdfsRoot.getRootPath(), new Path("file8.ext"));
    FSDataInputStream fstream = fs.open(newFile, 100);
    Assert.assertEquals(fs.openTimer.getCount(), 1);

    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testSetOwner() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/setOwner");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    Path newFile = new Path(hdfsRoot.getRootPath(), new Path("file8.ext"));
    fs.setOwner(newFile, "someone", "linkedin");
    Assert.assertEquals(fs.setOwnerTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testGetFileStatus() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/getFileStatus");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    fs.getFileStatus(hdfsRoot.getFilePath8());
    Assert.assertEquals(fs.getFileStatusTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testSetPermission() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/permission");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ);
    fs.setPermission(hdfsRoot.getFilePath8(), permission);
    Assert.assertEquals(fs.setPermissionTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testSetTimes() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/setTimes");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    fs.setTimes(hdfsRoot.getFilePath8(), System.currentTimeMillis(), System.currentTimeMillis());
    Assert.assertEquals(fs.setTimesTimer.getCount(), 1);
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testAppend1() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/append");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FSDataOutputStream fstream = fs.append(hdfsRoot.getFilePath8());
    Assert.assertEquals(fs.appendTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testAppend2() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/append");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FSDataOutputStream fstream = fs.append(hdfsRoot.getFilePath8(), 100);
    Assert.assertEquals(fs.appendTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }

  @Test(enabled = false)
  public void testAppend3() throws IOException, URISyntaxException {
    HDFSRoot hdfsRoot = new HDFSRoot("/tmp/append");
    MetricsFileSystemInstrumentation
        fs = (MetricsFileSystemInstrumentation) FileSystem.get(new URI(instrumentedURI), new Configuration());
    FSDataOutputStream fstream = fs.append(hdfsRoot.getFilePath8(), 100, null);
    Assert.assertEquals(fs.appendTimer.getCount(), 1);
    fstream.close();
    hdfsRoot.cleanupRoot();
  }
}
