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

import java.util.List;

import com.google.common.base.Optional;


/**
 * This class is necessary for unit test purpose:
 * - Mockito cannot mimic the situation where a file system contains a dataset path.
 * - In {@link CopyRouteGeneratorOptimizedNetworkBandwidth} we neet to check if the dataset is really on the filesystem.
 * - The {@link CopyRouteGeneratorTest} is testing for route selection,
 *    so no matter the dataset is indeed existed, the testing purpose can still be achieved.
 */
public class CopyRouteGeneratorOptimizedNetworkBandwidthForTest extends CopyRouteGeneratorOptimizer {
  @Override
  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    for (CopyRoute copyRoute : routes) {
      if (!(copyRoute.getCopyFrom() instanceof HadoopFsEndPoint)) {
        continue;
      }

      HadoopFsEndPoint copyFrom = (HadoopFsEndPoint) (copyRoute.getCopyFrom());
      if (copyFrom.isFileSystemAvailable()) {
        return Optional.of(copyRoute);
      }
    }
    return Optional.absent();
  }
}
