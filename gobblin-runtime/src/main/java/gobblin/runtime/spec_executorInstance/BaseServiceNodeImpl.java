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
package gobblin.runtime.spec_executorInstance;

import java.util.Properties;

import com.typesafe.config.Config;

import gobblin.runtime.api.ServiceNode;
import gobblin.util.ConfigUtils;

import lombok.Data;

@Data
/**
 * A base implementation for {@link ServiceNode} with default secured setting.
 */
public class BaseServiceNodeImpl implements ServiceNode {

  public String nodeName ;

  public Config nodeConfig;

  public static final String NODE_SECURITY_KEY = "node.secured";

  // True means node is secure.
  private static final String DEFAULT_NODE_SECURITY = "true";

  /**
   * For nodes missing configuration
   * @param nodeName
   */
  public BaseServiceNodeImpl(String nodeName) {
    this(nodeName, new Properties());
  }

  public BaseServiceNodeImpl(String nodeName, Properties props){
    this.nodeName = nodeName ;
    if (!props.containsKey(this.NODE_SECURITY_KEY)){
      props.setProperty(this.NODE_SECURITY_KEY, DEFAULT_NODE_SECURITY);
    }
    nodeConfig = ConfigUtils.propertiesToConfig(props);
  }
}
