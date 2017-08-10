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
package org.apache.gobblin.data.management.copy;

import java.io.IOException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;


/**
 * Utils class to generate dummy {@link CopyableFile}s for testing. Random strings are generated for null paths.
 */
public class CopyableFileUtils {

  public static CopyableFile createTestCopyableFile(String resourcePath) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.create(new Path(resourcePath));

    FileStatus status = new FileStatus(0l, false, 0, 0l, 0l, new Path(resourcePath));

    return new CopyableFile(status, new Path(getRandomPath()), null, null, null,
        PreserveAttributes.fromMnemonicString(""), "", 0 ,0, Maps.<String, String>newHashMap(), "");
  }

  public static CopyableFile getTestCopyableFile() {
    return getTestCopyableFile(null, null);
  }

  public static CopyableFile getTestCopyableFile(String resourcePath) {
    return getTestCopyableFile(resourcePath, null);
  }

  public static CopyableFile getTestCopyableFile(OwnerAndPermission ownerAndPermission) {
    return getTestCopyableFile(null, null, ownerAndPermission);
  }

  public static CopyableFile getTestCopyableFile(String resourcePath, OwnerAndPermission ownerAndPermission) {
    return getTestCopyableFile(resourcePath, null, ownerAndPermission);
  }

  public static CopyableFile getTestCopyableFile(String resourcePath, String relativePath,
      OwnerAndPermission ownerAndPermission) {
    return getTestCopyableFile(resourcePath, getRandomPath(), relativePath, ownerAndPermission);
  }

  public static CopyableFile getTestCopyableFile(String resourcePath, String destinationPath, String relativePath,
      OwnerAndPermission ownerAndPermission) {

    FileStatus status = null;

    if (resourcePath == null) {
      resourcePath = getRandomPath();
      status = new FileStatus(0l, false, 0, 0l, 0l, new Path(resourcePath));
    } else {
      String filePath = CopyableFileUtils.class.getClassLoader().getResource(resourcePath).getFile();
      status = new FileStatus(0l, false, 0, 0l, 0l, new Path(filePath));
    }

    if (relativePath == null) {
      relativePath = getRandomPath();
    }

    Path destinationRelativePath = new Path(relativePath);

    return new CopyableFile(status, new Path(destinationPath), ownerAndPermission, null, null,
        PreserveAttributes.fromMnemonicString(""), "", 0, 0, Maps.<String, String>newHashMap(), "");
  }

  private static String getRandomPath() {
    return new Path(RandomStringUtils.randomAlphabetic(6), RandomStringUtils.randomAlphabetic(6)).toString();
  }
}
