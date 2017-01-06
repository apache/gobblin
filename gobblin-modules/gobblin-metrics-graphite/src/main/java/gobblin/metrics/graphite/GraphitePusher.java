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

package gobblin.metrics.graphite;

import java.io.Closeable;
import java.io.IOException;

import com.codahale.metrics.graphite.GraphiteSender;
import com.google.common.io.Closer;

/**
 * Establishes a connection through the Graphite protocol and pushes timestamped name - value pairs
 *
 * @author Lorand Bendig
 *
 */
public class GraphitePusher implements Closeable {

  private GraphiteSender graphiteSender;
  private final Closer closer;

  public GraphitePusher(String hostname, int port, GraphiteConnectionType connectionType) throws IOException {
    this.closer = Closer.create();
    this.graphiteSender = this.closer.register(connectionType.createConnection(hostname, port));
    if (this.graphiteSender != null && !this.graphiteSender.isConnected()) {
      this.graphiteSender.connect();
    }
  }

  /**
   * Pushes a single metrics through the Graphite protocol to the underlying backend
   *
   * @param name metric name
   * @param value metric value
   * @param timestamp associated timestamp
   * @throws IOException
   */
  public void push(String name, String value, long timestamp) throws IOException {
    graphiteSender.send(name, value, timestamp);
  }

  public void flush() throws IOException {
    this.graphiteSender.flush();
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

}
