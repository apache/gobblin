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

package gobblin.service.modules.flow;

import java.util.Properties;

import static gobblin.service.ServiceConfigKeys.*;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class FlowEdgeProps {

  private Properties properties;

  private static final double DEFAULT_EDGE_LOAD = 1.0;
  private static final boolean DEFAULT_EDGE_SAFETY = true;

  public FlowEdgeProps(Properties props){
    this.properties = props;
  }

  /**
   * When initializing an edge, load and security value from properties will be used
   * but could be overriden afterwards.
   */
  public boolean getInitialEdgeSafety(){
    return  properties.containsKey(EDGE_SECURITY_KEY) ?
        Boolean.parseBoolean(properties.getProperty(EDGE_SECURITY_KEY)) : DEFAULT_EDGE_SAFETY;
  }

}
