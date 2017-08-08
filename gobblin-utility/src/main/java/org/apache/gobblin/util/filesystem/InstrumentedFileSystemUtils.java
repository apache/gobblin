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

package org.apache.gobblin.util.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


/**
 * Common methods for Instrumented {@link org.apache.hadoop.fs.FileSystem}s.
 */
public class InstrumentedFileSystemUtils {

  /**
   * Replace the scheme of the input {@link URI} if it matches the string to replace.
   */
  public static URI replaceScheme(URI uri, String replace, String replacement) {
    if (replace != null && replace.equals(replacement)) {
      return uri;
    }
    try {
      if (replace != null && replace.equals(uri.getScheme())) {
        return new URI(replacement, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
      } else {
        return uri;
      }
    } catch (URISyntaxException use) {
      throw new RuntimeException("Failed to replace scheme.");
    }
  }

  /**
   * Replace the scheme of the input {@link Path} if it matches the string to replace.
   */
  public static Path replaceScheme(Path path, String replace, String replacement) {
    return new Path(replaceScheme(path.toUri(), replace, replacement));
  }

  /**
   * Replace the scheme of each {@link Path} if it matches the string to replace.
   */
  public static Path[] replaceScheme(Path[] paths, String replace, String replacement) {
    if (replace != null && replace.equals(replacement)) {
      return paths;
    }
    Path[] output = new Path[paths.length];
    for (int i = 0; i < paths.length; i++) {
      output[i] = replaceScheme(paths[i], replace, replacement);
    }
    return output;
  }

  /**
   * Replace the scheme of each {@link FileStatus} if it matches the string to replace.
   */
  public static FileStatus[] replaceScheme(FileStatus[] paths, String replace, String replacement) {
    if (replace != null && replace.equals(replacement)) {
      return paths;
    }
    FileStatus[] output = new FileStatus[paths.length];
    for (int i = 0; i < paths.length; i++) {
      output[i] = replaceScheme(paths[i], replace, replacement);
    }
    return output;
  }

  /**
   * Replace the scheme of the input {@link FileStatus} if it matches the string to replace.
   */
  public static FileStatus replaceScheme(FileStatus st, String replace, String replacement) {
    if (replace != null && replace.equals(replacement)) {
      return st;
    }
    try {
      return new FileStatus(st.getLen(), st.isDir(), st.getReplication(), st.getBlockSize(), st.getModificationTime(),
          st.getAccessTime(), st.getPermission(), st.getOwner(), st.getGroup(), st.isSymlink() ? st.getSymlink() : null,
          replaceScheme(st.getPath(), replace, replacement));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

}
