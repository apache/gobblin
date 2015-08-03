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

package gobblin.data.management.trash;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.util.ProxiedFileSystemCache;


/**
 * {@link gobblin.data.management.trash.ProxiedTrash} implementation that immediately deletes
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
        ProxiedFileSystemCache.getProxiedFileSystem(user, properties, fs.getUri()), properties, user);
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
