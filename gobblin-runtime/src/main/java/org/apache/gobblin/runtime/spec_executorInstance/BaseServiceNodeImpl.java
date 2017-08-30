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
package org.apache.gobblin.runtime.spec_executorInstance;

import java.util.Properties;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.Setter;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Getter;

/**
 * A base implementation for {@link ServiceNode} with default secured setting.
 */
public class BaseServiceNodeImpl implements ServiceNode {

  @Getter
  public String nodeName;

  /**
   * Contains read-only properties of an {@link ServiceNode}
   */
  @Getter
  public Config nodeProps;

  /**
   * One of mutable properties of Node.
   * Initialization: Obtained from {@link ServiceConfigKeys}.
   * Getter/Setter: Simply thur. {@link BaseServiceNodeImpl}.
   */
  @Getter
  @Setter
  private boolean isNodeSecure;

  /**
   * For nodes missing configuration
   * @param nodeName
   */
  public BaseServiceNodeImpl(String nodeName) {
    this(nodeName, new Properties());
  }

  public BaseServiceNodeImpl(String nodeName, Properties props) {
    Preconditions.checkNotNull(nodeName);
    this.nodeName = nodeName;
    isNodeSecure = Boolean.parseBoolean
        (props.getProperty(ServiceConfigKeys.NODE_SECURITY_KEY, ServiceConfigKeys.DEFAULT_NODE_SECURITY));
    nodeProps = ConfigUtils.propertiesToConfig(props);
  }

  /**
   * By default each node is acceptable to use in path-finding.
   */
  @Override
  public boolean isNodeEnabled() {
    return true;
  }

  /**
   * The comparison between two nodes should involve the configuration.
   * Node name is the identifier for the node.
   * */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseServiceNodeImpl that = (BaseServiceNodeImpl) o;

    return nodeName.equals(that.nodeName);
  }

  @Override
  public int hashCode() {
    return nodeName.hashCode();
  }
}