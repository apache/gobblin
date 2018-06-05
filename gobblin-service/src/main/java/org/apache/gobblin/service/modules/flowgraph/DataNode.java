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

package org.apache.gobblin.service.modules.flowgraph;

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;


/**
 * Representation of a node in the FlowGraph. Each node is identified by a unique identifier.
 */
@Alpha
public interface DataNode {
  /**
   * @return the identifier of a {@link DataNode}.
   */
  String getId();

  /**
   * @return the attributes of a {@link DataNode}. It also includes properties for resolving a {@link org.apache.gobblin.runtime.api.JobTemplate}
   * e.g. "source.fs.uri" for an HDFS node, "jdbc.publisher.url" for JDBC node.
   */
  Config getProps();

  /**
   * @return true if the {@link DataNode} is active
   */
  boolean isActive();

  class DataNodeCreationException extends Exception {
    private static final String MESSAGE_FORMAT = "Failed to create DataNode because of: %s";

    public DataNodeCreationException(Exception e) {
      super(String.format(MESSAGE_FORMAT, e.getMessage()), e);
    }
  }
}
