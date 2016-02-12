/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.net.URI;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Strings;


public class PathUtils {

  public static final Pattern GLOB_TOKENS = Pattern.compile("[\\?\\*\\[\\{]");

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
   * @see Path
   * @see URI
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
   * Finds the deepest ancestor of input that is not a glob.
   */
  public static Path deepestNonGlobPath(Path input) {
    Path commonRoot = input;

    while(commonRoot != null && GLOB_TOKENS.matcher(commonRoot.toString()).find()) {
      commonRoot = commonRoot.getParent();
    }
    return commonRoot;
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
  public static Path removeExtension(Path path, String...extensions) {
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
  public static Path addExtension(Path path, String...extensions) {
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
}
