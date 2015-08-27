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
   * Removes the Scheme and Authority from a Path.
   *
   * @see {@link Path}, {@link URI}
   */
  public static Path getPathWithoutSchemeAndAuthority(Path path) {
    return new Path(null, null, path.toUri().getPath());
  }
}
