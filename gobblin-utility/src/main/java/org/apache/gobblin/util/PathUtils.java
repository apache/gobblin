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

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PathUtils {

  public static final Pattern GLOB_TOKENS = Pattern.compile("[,\\?\\*\\[\\{]");

  public static Path mergePaths(Path path1, Path path2) {
    String path2Str = path2.toUri().getPath();
    if (!path2Str.startsWith("/")) {
      path2Str = "/" + path2Str;
    }
    return new Path(path1.toUri().getScheme(), path1.toUri().getAuthority(), path1.toUri().getPath() + path2Str);
  }

  public static Path relativizePath(Path fullPath, Path pathPrefix) {
    return new Path(getPathWithoutSchemeAndAuthority(pathPrefix).toUri()
        .relativize(getPathWithoutSchemeAndAuthority(fullPath).toUri()));
  }

  /**
   * Checks whether possibleAncestor is an ancestor of fullPath.
   * @param possibleAncestor Possible ancestor of fullPath.
   * @param fullPath path to check.
   * @return true if possibleAncestor is an ancestor of fullPath.
   */
  public static boolean isAncestor(Path possibleAncestor, Path fullPath) {
    return !relativizePath(fullPath, possibleAncestor).equals(getPathWithoutSchemeAndAuthority(fullPath));
  }

  /**
   * Removes the Scheme and Authority from a Path.
   *
   * @see Path
   * @see URI
   */
  public static Path getPathWithoutSchemeAndAuthority(Path path) {
    return new Path(null, null, path.toUri().getPath());
  }

  /**
   * Returns the root path for the specified path.
   *
   * @see Path
   */
  public static Path getRootPath(Path path) {
    if (path.isRoot()) {
      return path;
    }
    return getRootPath(path.getParent());
  }

  /**
   * Removes the leading slash if present.
   *
   */
  public static Path withoutLeadingSeparator(Path path) {
    return new Path(StringUtils.removeStart(path.toString(), Path.SEPARATOR));
  }

  /**
   * Finds the deepest ancestor of input that is not a glob.
   */
  public static Path deepestNonGlobPath(Path input) {
    Path commonRoot = input;

    while (commonRoot != null && isGlob(commonRoot)) {
      commonRoot = commonRoot.getParent();
    }
    return commonRoot;
  }

  /**
   * @return true if path has glob tokens (e.g. *, {, \, }, etc.)
   */
  public static boolean isGlob(Path path) {
    return (path != null) && GLOB_TOKENS.matcher(path.toString()).find();
  }

  /**
   * Removes all <code>extensions</code> from <code>path</code> if they exist.
   *
   * <pre>
   * PathUtils.removeExtention("file.txt", ".txt")                      = file
   * PathUtils.removeExtention("file.txt.gpg", ".txt", ".gpg")          = file
   * PathUtils.removeExtention("file", ".txt")                          = file
   * PathUtils.removeExtention("file.txt", ".tar.gz")                   = file.txt
   * PathUtils.removeExtention("file.txt.gpg", ".txt")                  = file.gpg
   * PathUtils.removeExtention("file.txt.gpg", ".gpg")                  = file.txt
   * </pre>
   *
   * @param path in which the <code>extensions</code> need to be removed
   * @param extensions to be removed
   *
   * @return a new {@link Path} without <code>extensions</code>
   */
  public static Path removeExtension(Path path, String... extensions) {
    String pathString = path.toString();
    for (String extension : extensions) {
      pathString = StringUtils.remove(pathString, extension);
    }

    return new Path(pathString);
  }

  /**
   * Suffix all <code>extensions</code> to <code>path</code>.
   *
   * <pre>
   * PathUtils.addExtension("/tmp/data/file", ".txt")                          = file.txt
   * PathUtils.addExtension("/tmp/data/file.txt.gpg", ".zip")                  = file.txt.gpg.zip
   * PathUtils.addExtension("/tmp/data/file.txt", ".tar", ".gz")               = file.txt.tar.gz
   * PathUtils.addExtension("/tmp/data/file.txt.gpg", ".tar.txt")              = file.txt.gpg.tar.txt
   * </pre>
   *
   * @param path to which the <code>extensions</code> need to be added
   * @param extensions to be added
   *
   * @return a new {@link Path} with <code>extensions</code>
   */
  public static Path addExtension(Path path, String... extensions) {
    StringBuilder pathStringBuilder = new StringBuilder(path.toString());
    for (String extension : extensions) {
      if (!Strings.isNullOrEmpty(extension)) {
        pathStringBuilder.append(extension);
      }
    }
    return new Path(pathStringBuilder.toString());
  }

  public static Path combinePaths(String... paths) {
    if (paths.length == 0) {
      throw new IllegalArgumentException("Paths cannot be empty!");
    }

    Path path = new Path(paths[0]);
    for (int i = 1; i < paths.length; i++) {
      path = new Path(path, paths[i]);
    }
    return path;
  }

  /**
   * Is an absolute path (ie a slash relative path part)
   *  AND  a scheme is null AND  authority is null.
   */
  public static boolean isAbsoluteAndSchemeAuthorityNull(Path path) {
    return (path.isAbsolute() &&
        path.toUri().getScheme() == null && path.toUri().getAuthority() == null);
  }

  /**
   * Deletes empty directories starting with startPath and all ancestors up to but not including limitPath.
   * @param fs {@link FileSystem} where paths are located.
   * @param limitPath only {@link Path}s that are strict descendants of this path will be deleted.
   * @param startPath first {@link Path} to delete. Afterwards empty ancestors will be deleted.
   * @throws IOException
   */
  public static void deleteEmptyParentDirectories(FileSystem fs, Path limitPath, Path startPath)
      throws IOException {
    if (PathUtils.isAncestor(limitPath, startPath) && !PathUtils.getPathWithoutSchemeAndAuthority(limitPath)
        .equals(PathUtils.getPathWithoutSchemeAndAuthority(startPath)) && fs.listStatus(startPath).length == 0) {
      if (!fs.delete(startPath, false)) {
        log.warn("Failed to delete empty directory " + startPath);
      } else {
        log.info("Deleted empty directory " + startPath);
      }
      deleteEmptyParentDirectories(fs, limitPath, startPath.getParent());
    }
  }

  /**
   * Compare two path without shedme and authority (the prefix)
   * @param path1
   * @param path2
   * @return
   */
  public static boolean compareWithoutSchemeAndAuthority(Path path1, Path path2) {
    return PathUtils.getPathWithoutSchemeAndAuthority(path1).equals(getPathWithoutSchemeAndAuthority(path2));
  }
}
