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

import java.util.Collection;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;


/**
 * A class for obtaining record counts from data.
 */
public abstract class RecordCountProvider {

  /**
   * Get record count from a given {@link Path}.
   */
  public abstract long getRecordCount(Path path);

  /**
   * Convert a {@link Path} from another {@link RecordCountProvider} so that it can be used
   * in {@link #getRecordCount(Path)} of this {@link RecordCountProvider}.
   */
  public Path convertPath(Path path, String extension, RecordCountProvider src) {
    if (this.getClass().equals(src.getClass())) {
      return path;
    }
    throw getNotImplementedException(src);
  }

  protected NotImplementedException getNotImplementedException(RecordCountProvider src) {
    return new NotImplementedException(String.format("converting from %s to %s is not implemented",
        src.getClass().getName(), this.getClass().getName()));
  }

  /**
   * Get record count for a list of paths.
   */
  public long getRecordCount(Collection<Path> paths) {
    long count = 0;
    for (Path path : paths) {
      count += getRecordCount(path);
    }
    return count;
  }
}
