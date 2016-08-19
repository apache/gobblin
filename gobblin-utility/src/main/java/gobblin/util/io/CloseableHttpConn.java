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
import java.net.HttpURLConnection;

import lombok.AllArgsConstructor;


/**
 * Wraps a {@link HttpURLConnection} into a {@link Closeable} object.
 */
@AllArgsConstructor
public class CloseableHttpConn implements Closeable{
  private final HttpURLConnection connection;
  @Override
  public void close()
      throws IOException {
    if (this.connection != null) {
      this.connection.disconnect();
    }
  }
}
