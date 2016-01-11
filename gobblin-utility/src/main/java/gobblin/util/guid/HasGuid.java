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

package gobblin.util.guid;

import java.io.IOException;


/**
 * Represents an object for which we can compute a unique, replicable {@link Guid}.
 */
public interface HasGuid {

  /**
   * @return the {@link Guid} for this object.
   * @throws IOException
   */
  public Guid guid() throws IOException;

}
