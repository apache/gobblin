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


import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;


@Test(groups = { "gobblin.data.management.copy.replication" })
public class CopyRouteGeneratorTest {

  @Test
  public void testCopyRouteGenerator() throws Exception {
    long replica1Watermark = 1475304606000L; // Oct 1, 2016
    long sourceWatermark   = 1475604606000L; // Oct 4, 2016

    ReplicaHadoopFsEndPoint notAvailableReplica = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(notAvailableReplica.isFileSystemAvailable()).thenReturn(false);
    Optional<ComparableWatermark> tmp = Optional.absent();
    Mockito.when(notAvailableReplica.getWatermark()).thenReturn(tmp);

    ReplicaHadoopFsEndPoint replica1 = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(replica1.isFileSystemAvailable()).thenReturn(true);
    ComparableWatermark cw = new LongWatermark(replica1Watermark) ;
    tmp = Optional.of(cw);
    Mockito.when(replica1.getWatermark()).thenReturn(tmp);

    SourceHadoopFsEndPoint source = Mockito.mock(SourceHadoopFsEndPoint.class);
    Mockito.when(source.isFileSystemAvailable()).thenReturn(true);
    cw = new LongWatermark(sourceWatermark);
    tmp = Optional.of(cw);
    Mockito.when(source.getWatermark()).thenReturn(tmp);

    ReplicaHadoopFsEndPoint copyToEndPoint = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(copyToEndPoint.isFileSystemAvailable()).thenReturn(true);

    CopyRoute cp1 = new CopyRoute(notAvailableReplica, copyToEndPoint);
    CopyRoute cp2 = new CopyRoute(replica1, copyToEndPoint);
    CopyRoute cp3 = new CopyRoute(source, copyToEndPoint);
    DataFlowTopology.DataFlowPath dataFlowPath =
        new DataFlowTopology.DataFlowPath(ImmutableList.<CopyRoute> of(cp1, cp2, cp3));
    DataFlowTopology dataFlowTopology = new DataFlowTopology();
    dataFlowTopology.addDataFlowPath(dataFlowPath);

    ReplicationConfiguration rc = Mockito.mock(ReplicationConfiguration.class);
    Mockito.when(rc.getCopyMode()).thenReturn(ReplicationCopyMode.PULL);
    Mockito.when(rc.getSource()).thenReturn(source);
    Mockito.when(rc.getReplicas()).thenReturn(ImmutableList.<EndPoint> of(notAvailableReplica, replica1, copyToEndPoint));
    Mockito.when(rc.getDataFlowToplogy()).thenReturn(dataFlowTopology);

    CopyRouteGeneratorOptimizedNetworkBandwidthForTest network = new CopyRouteGeneratorOptimizedNetworkBandwidthForTest();

    Assert.assertTrue(network.getPullRoute(rc, copyToEndPoint).get().getCopyFrom().equals(replica1));
    Assert.assertTrue(network.getPullRoute(rc, copyToEndPoint).get().getCopyFrom().getWatermark()
        .get().compareTo(new LongWatermark(replica1Watermark)) == 0);

    CopyRouteGeneratorOptimizedLatency latency = new CopyRouteGeneratorOptimizedLatency();
    Assert.assertTrue(latency.getPullRoute(rc, copyToEndPoint).get().getCopyFrom().equals(source));
    Assert.assertTrue(latency.getPullRoute(rc, copyToEndPoint).get().getCopyFrom().getWatermark()
        .get().compareTo(new LongWatermark(sourceWatermark)) == 0);
  }
}
