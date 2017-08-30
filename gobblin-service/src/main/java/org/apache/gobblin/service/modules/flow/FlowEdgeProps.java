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

package org.apache.gobblin.service.modules.flow;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.io.Serializable;
import java.util.Properties;

import static org.apache.gobblin.service.ServiceConfigKeys.*;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlowEdgeProps {
  protected static final boolean DEFAULT_EDGE_SAFETY = true;

  /**
   * Contains read-only properties that users want to package in.
   */
  @Getter
  protected Config config;

  /**
   * One of the mutable properties of an edge.
   */
  @Getter
  @Setter
  private boolean isEdgeSecure;

  public FlowEdgeProps(Config config) {
    this.config = config;
    isEdgeSecure = getInitialEdgeSafety();
  }

  public FlowEdgeProps() {
    this(ConfigFactory.empty());
  }

  /**
   * When initializing an edge, load and security value from properties will be used
   * but could be overriden afterwards.
   */
  private boolean getInitialEdgeSafety() {
    return
        config.hasPath(EDGE_SECURITY_KEY) ? config.getBoolean(EDGE_SECURITY_KEY) : DEFAULT_EDGE_SAFETY;
  }
}