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
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class FileUtils {
  public void saveToFile(final String text, final Path destPath)
      throws IOException {
    try (PrintWriter out = new PrintWriter(
        Files.newBufferedWriter(destPath, StandardCharsets.UTF_8))) {
      out.println(text);
      out.flush();
    }
  }

  /***
   * Check if child path is child of parent path.
   * @param parent Expected parent path.
   * @param child Expected child path.
   * @return If child path is child of parent path.
   * @throws IOException
   */
  public static boolean isSubPath(File parent, File child) throws IOException {
    String childStr = child.getCanonicalPath();
    String parentStr = parent.getCanonicalPath();

    return childStr.startsWith(parentStr);
  }

  /***
   * Check if child path is child of parent path.
   * @param parent Expected parent path.
   * @param child Expected child path.
   * @return If child path is child of parent path.
   * @throws IOException
   */
  public static boolean isSubPath(org.apache.hadoop.fs.Path parent, org.apache.hadoop.fs.Path child) throws IOException {
    String childStr = child.toString();
    String parentStr = parent.toString();

    return childStr.startsWith(parentStr);
  }
}
