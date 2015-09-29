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

import org.apache.hadoop.fs.Path;


/**
 * Interface for proxy enabled trash.
 */
public interface GobblinProxiedTrash extends GobblinTrash {

  /**
   * Move the path to trash as specified user.
   * @param path {@link org.apache.hadoop.fs.Path} to move.
   * @param user User to move the path as.
   * @return true if the move succeeded.
   * @throws IOException
   */
  public boolean moveToTrashAsUser(Path path, final String user) throws IOException;

}
