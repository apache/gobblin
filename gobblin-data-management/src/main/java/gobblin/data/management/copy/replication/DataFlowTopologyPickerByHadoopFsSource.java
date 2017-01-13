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

package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;

@Alias(value="DataFlowTopologyPickerByHadoopFsSource")
public class DataFlowTopologyPickerByHadoopFsSource implements DataFlowTopologyPickerBySource {

  @Override
  public Config getPreferredRoutes(Config allTopologies, EndPoint source) {
    Preconditions.checkArgument(source instanceof HadoopFsEndPoint,
        "source is NOT expectecd class " + HadoopFsEndPoint.class.getCanonicalName());

    HadoopFsEndPoint hadoopFsSource = (HadoopFsEndPoint)source;
    String clusterName = hadoopFsSource.getClusterName();

    Preconditions.checkArgument(allTopologies.hasPath(clusterName),
        "Can not find preferred topology for cluster name " + clusterName);
    return allTopologies.getConfig(clusterName);
  }
}
