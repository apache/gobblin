/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;


public class PathUtils {

  public static Path mergePaths(Path path1, Path path2) {
    String path2Str = path2.toUri().getPath();
    return new Path(path1.toUri().getScheme(), path1.toUri().getAuthority(), path1.toUri().getPath() + path2Str);
  }

  public static Path relativizePath(Path fullPath, Path pathPrefix) {
    return new Path(pathPrefix.toUri().relativize(fullPath.toUri()));
  }

  /**
   * Checks whether possibleAncestor is an ancestor of fullPath.
   * @param possibleAncestor Possible ancestor of fullPath.
   * @param fullPath path to check.
   * @return true if possibleAncestor is an ancestor of fullPath.
   */
  public static boolean isAncestor(Path possibleAncestor, Path fullPath) {
    return !relativizePath(fullPath, possibleAncestor).equals(fullPath);
  }

  /**
   * Removes the Scheme and Authority from a Path.
   *
   * @see {@link Path}, {@link URI}
   */
  public static Path getPathWithoutSchemeAndAuthority(Path path) {
    return new Path(null, null, path.toUri().getPath());
  }

  /**
   * Removes the leading slash if present.
   *
   */
  public static Path withoutLeadingSeparator(Path path) {
    return new Path(StringUtils.removeStart(path.toString(), Path.SEPARATOR));
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
  public static Path removeExtention(Path path, String...extensions) {
    String pathString = path.toString();
    for (String extension : extensions) {
      pathString = StringUtils.remove(pathString, extension);
    }

    return new Path(pathString);
  }
}
