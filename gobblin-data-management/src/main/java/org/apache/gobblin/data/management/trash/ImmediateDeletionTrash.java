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

package org.apache.gobblin.data.management.trash;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.util.ProxiedFileSystemCache;


/**
 * {@link org.apache.gobblin.data.management.trash.ProxiedTrash} implementation that immediately deletes
 * {@link org.apache.hadoop.fs.Path}s instead of moving them to trash.
 */
public class ImmediateDeletionTrash extends ProxiedTrash {

  public ImmediateDeletionTrash(FileSystem fs, Properties props, String user)
      throws IOException {
    super(fs, props, user);
  }

  @Override
  protected Trash createNewTrashForUser(FileSystem fs, Properties properties, String user)
      throws IOException {
    return new ImmediateDeletionTrash(
        ProxiedFileSystemCache.fromProperties().userNameToProxyAs(user).properties(properties).referenceFS(fs).build(),
        properties, user);
  }

  @Override
  public boolean moveToTrash(Path path)
      throws IOException {
    return this.fs.delete(path, true);
  }

  @Override
  protected void ensureTrashLocationExists(FileSystem fs, Path trashLocation)
      throws IOException {
    // Do nothing
  }
}
