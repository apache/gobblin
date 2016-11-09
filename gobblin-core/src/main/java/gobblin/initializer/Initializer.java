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

package gobblin.initializer;

import java.io.Closeable;

public interface Initializer extends Closeable {

  /**
   * Initialize for the writer.
   *
   * @param state
   * @param workUnits WorkUnits created by Source
   */
  public void initialize();

  /**
   * Removed checked exception.
   * {@inheritDoc}
   * @see java.io.Closeable#close()
   */
  @Override
  public void close();
}
