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

package org.apache.gobblin.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class FileUtilsTest {

  @Test
  public void testSaveToFile()
      throws IOException {
    FileUtils utils = new FileUtils();
    Path destPath = Paths.get("fileUtilTest.txt");
    utils.saveToFile("foo", destPath);

    assertThat(destPath).exists().isReadable().hasContent("foo\n");

    Files.deleteIfExists(destPath);
  }

  @Test
  public void testIsSubFile() throws IOException {
    File parentPath = new File("/tmp/foo/bar");

    File childPath = new File("/tmp/foo/../tar/file.txt");
    assertThat(false).isEqualTo(FileUtils.isSubPath(parentPath, childPath));

    childPath = new File("/tmp/foo/tar/../bar/file.txt");
    assertThat(true).isEqualTo(FileUtils.isSubPath(parentPath, childPath));

    childPath = new File("/tmp/foo/bar/car/file.txt");
    assertThat(true).isEqualTo(FileUtils.isSubPath(parentPath, childPath));
  }

  @Test
  public void testIsSubPath() throws IOException {
    org.apache.hadoop.fs.Path parentPath = new org.apache.hadoop.fs.Path("/tmp/foo/bar");

    org.apache.hadoop.fs.Path childPath = new org.apache.hadoop.fs.Path("/tmp/foo/../tar/file.txt");
    assertThat(false).isEqualTo(FileUtils.isSubPath(parentPath, childPath));

    childPath = new org.apache.hadoop.fs.Path("/tmp/foo/tar/../bar/file.txt");
    assertThat(true).isEqualTo(FileUtils.isSubPath(parentPath, childPath));

    childPath = new org.apache.hadoop.fs.Path("/tmp/foo/bar/car/file.txt");
    assertThat(true).isEqualTo(FileUtils.isSubPath(parentPath, childPath));
  }
}
