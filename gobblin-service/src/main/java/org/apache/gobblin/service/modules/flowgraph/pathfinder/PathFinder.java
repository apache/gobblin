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

package org.apache.gobblin.service.modules.flowgraph.pathfinder;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flow.FlowGraphPath;


/**
 * An interface for computing a path in a {@link org.apache.gobblin.service.modules.flowgraph.FlowGraph}. Each
 * implementation of {@link PathFinder} implements a specific path finding algorithm such as Breadth-First Search (BFS),
 * Dijkstra's shortest-path algorithm etc.
 */
@Alpha
public interface PathFinder {

  public FlowGraphPath findPath() throws PathFinderException;

  public static class PathFinderException extends Exception {
    public PathFinderException(String message, Throwable cause) {
      super(message, cause);
    }

    public PathFinderException(String message) {
      super(message);
    }
  }

}
