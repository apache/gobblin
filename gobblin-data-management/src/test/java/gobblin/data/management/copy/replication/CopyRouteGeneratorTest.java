/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.replication;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import gobblin.source.extractor.extract.LongWatermark;


@Test(groups = { "gobblin.data.management.copy.replication" })
public class CopyRouteGeneratorTest {

  @Test
  public void testCopyRouteGenerator() throws Exception {
    long unoWatermark = 1475304606000L;
    long sourceWatermark = 1475604606000L;

    ReplicaHadoopFsEndPoint holdemReplica = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(holdemReplica.isAvailable()).thenReturn(false);

    ReplicaHadoopFsEndPoint unoReplica = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(unoReplica.isAvailable()).thenReturn(true);
    Mockito.when(unoReplica.getWatermark()).thenReturn(new LongWatermark(unoWatermark)); // Oct 1, 2016

    SourceHadoopFsEndPoint source = Mockito.mock(SourceHadoopFsEndPoint.class);
    Mockito.when(source.isAvailable()).thenReturn(true);
    Mockito.when(source.getWatermark()).thenReturn(new LongWatermark(sourceWatermark)); // Oct 4, 2016

    ReplicaHadoopFsEndPoint warReplica = Mockito.mock(ReplicaHadoopFsEndPoint.class);
    Mockito.when(warReplica.isAvailable()).thenReturn(true);

    CopyRoute cp1 = new CopyRoute(holdemReplica, warReplica);
    CopyRoute cp2 = new CopyRoute(unoReplica, warReplica);
    CopyRoute cp3 = new CopyRoute(source, warReplica);
    DataFlowTopology.DataFlowPath dataFlowPath =
        new DataFlowTopology.DataFlowPath(ImmutableList.<CopyRoute> of(cp1, cp2, cp3));
    DataFlowTopology dataFlowTopology = new DataFlowTopology();
    dataFlowTopology.addDataFlowPath(dataFlowPath);

    ReplicationConfiguration rc = Mockito.mock(ReplicationConfiguration.class);
    Mockito.when(rc.getCopyMode()).thenReturn(ReplicationCopyMode.PULL);
    Mockito.when(rc.getSource()).thenReturn(source);
    Mockito.when(rc.getReplicas()).thenReturn(ImmutableList.<EndPoint> of(holdemReplica, unoReplica, warReplica));
    Mockito.when(rc.getDataFlowToplogy()).thenReturn(dataFlowTopology);

    CopyRouteGeneratorOptimizedNetworkBandwidth network = new CopyRouteGeneratorOptimizedNetworkBandwidth();
    Assert.assertTrue(network.getPullRoute(rc, warReplica).get().getCopyFrom().equals(unoReplica));
    Assert.assertTrue(network.getPullRoute(rc, warReplica).get().getCopyFrom().getWatermark()
        .compareTo(new LongWatermark(unoWatermark)) == 0);

    CopyRouteGeneratorOptimizedLatency latency = new CopyRouteGeneratorOptimizedLatency();
    Assert.assertTrue(latency.getPullRoute(rc, warReplica).get().getCopyFrom().equals(source));
    Assert.assertTrue(latency.getPullRoute(rc, warReplica).get().getCopyFrom().getWatermark()
        .compareTo(new LongWatermark(sourceWatermark)) == 0);
  }
}
