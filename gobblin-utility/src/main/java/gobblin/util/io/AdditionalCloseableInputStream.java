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

package gobblin.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;


/**
 * An extension of {@link FSDataInputStream} that takes additional {@link Closeable} objects,
 * which will all be closed when {@link InputStream} is closed.
 */
public class AdditionalCloseableInputStream extends FSDataInputStream {

  private Closeable[] closeables;

  public AdditionalCloseableInputStream(InputStream in, Closeable... closeables)
      throws IOException {
    super(in);
    this.closeables = closeables;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    for (Closeable closeable : this.closeables) {
      closeable.close();
    }
  }
}
