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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Test(groups = {"gobblin.data.management.copy.replication"})
public class ReplicationConfigurationTest {

  @Test
  public void testValidConfigsInPullMode() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    this.checkReplicationConfig_Pull(rc);
  }

  @Test
  public void testValidConfigsInPullMode_withTopologyPicker() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset_PullMode2.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    this.checkReplicationConfig_Pull(rc);
  }

  @Test
  public void testValidConfigsInPushMode_withClusterResolve() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset_PushMode.conf").resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    this.checkReplicationConfig_Push(rc);
  }

  @Test
  public void testValidConfigsInPushMode_withTopologyPicker() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset_PushMode2.conf").resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    this.checkReplicationConfig_Push(rc);
  }

  private void checkReplicationConfigStaticPart(ReplicationConfiguration rc) throws Exception{
    ReplicationMetaData md = rc.getMetaData();
    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_JIRA).equals("jira-4455"));
    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_NAME).equals("profileTest"));
    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_OWNER).equals("mitu"));

    EndPoint sourceTmp = rc.getSource();
    Assert.assertTrue(sourceTmp instanceof SourceHadoopFsEndPoint);
    SourceHadoopFsEndPoint source = (SourceHadoopFsEndPoint)sourceTmp;
    Assert.assertTrue(source.getEndPointName().equals(ReplicationConfiguration.REPLICATION_SOURCE));
    Assert.assertTrue(source.isSource());

    HadoopFsReplicaConfig innerConf = source.getRc();
    Assert.assertTrue(innerConf.getClustername().equals("cluster1"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://coloX-cluster1nn01.grid.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(innerConf.getColo().equals("coloX"));

    List<EndPoint> replicas = rc.getReplicas();
    Assert.assertTrue(replicas.size()==4);
    EndPoint replica_holdem = replicas.get(0);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_holdem).getRc();

    Assert.assertTrue(innerConf.getClustername().equals("cluster4"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://coloY-cluster4nn01.grid.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/oncluster4"));
    Assert.assertTrue(innerConf.getColo().equals("coloY"));

    EndPoint replica_uno = replicas.get(1);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_uno).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("cluster3"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://coloY-cluster3nn01.grid.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/oncluster3"));
    Assert.assertTrue(innerConf.getColo().equals("coloY"));

    EndPoint replica_war = replicas.get(2);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_war).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("cluster1"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://coloX-cluster1nn01.grid.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/oncluster1"));
    Assert.assertTrue(innerConf.getColo().equals("coloX"));

    EndPoint replica_tarock = replicas.get(3);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_tarock).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("cluster2"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://coloX-cluster2nn01.grid.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/oncluster2"));
    Assert.assertTrue(innerConf.getColo().equals("coloX"));
  }

  private void checkReplicationConfig_Push(ReplicationConfiguration rc) throws Exception{
    ReplicationCopyMode copyMode = rc.getCopyMode();
    Assert.assertTrue(copyMode == ReplicationCopyMode.PUSH);

    checkReplicationConfigStaticPart(rc);

    List<EndPoint> replicas = rc.getReplicas();
    EndPoint source = rc.getSource();
    EndPoint replica4 = replicas.get(0);
    EndPoint replica3 = replicas.get(1);
    EndPoint replica1 = replicas.get(2);
    EndPoint replica2 = replicas.get(3);

    DataFlowTopology topology = rc.getDataFlowToplogy();
    Assert.assertTrue(topology.getDataFlowPaths().size()==4);

    for(DataFlowTopology.DataFlowPath p: topology.getDataFlowPaths()){
      List<CopyRoute> pairs = p.getCopyRoutes();
      Assert.assertTrue(!pairs.isEmpty());
      String copyFromName = pairs.get(0).getCopyFrom().getEndPointName();

      if(copyFromName.equals("replica1")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica1 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica2 );
      }
      else if(copyFromName.equals("replica2")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica2 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica4 );
      }
      else if(copyFromName.equals("replica4")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica4 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica3 );
      }
      else if(copyFromName.equals(ReplicationConfiguration.REPLICATION_SOURCE)){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica1 );
      }
      else{
        throw new Exception("CopyFrom name is invalid " +copyFromName);
      }
    }
  }

  private void checkReplicationConfig_Pull(ReplicationConfiguration rc) throws Exception{

    ReplicationCopyMode copyMode = rc.getCopyMode();
    Assert.assertTrue(copyMode == ReplicationCopyMode.PULL);

    checkReplicationConfigStaticPart(rc);

    List<EndPoint> replicas = rc.getReplicas();
    EndPoint source = rc.getSource();
    EndPoint replica4 = replicas.get(0);
    EndPoint replica3 = replicas.get(1);
    EndPoint replica1 = replicas.get(2);
    EndPoint replica2 = replicas.get(3);

    DataFlowTopology topology = rc.getDataFlowToplogy();
    Assert.assertTrue(topology.getDataFlowPaths().size()==4);

    for(DataFlowTopology.DataFlowPath p: topology.getDataFlowPaths()){
      List<CopyRoute> pairs = p.getCopyRoutes();
      Assert.assertTrue(!pairs.isEmpty());
      String copyToName = pairs.get(0).getCopyTo().getEndPointName();

      if(copyToName.equals("replica1")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica1 );
      }
      else if(copyToName.equals("replica2")){
        Assert.assertTrue(pairs.size()==2);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica1 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica2 );

        Assert.assertTrue(pairs.get(1).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica2 );
      }
      else if(copyToName.equals("replica4")){
        Assert.assertTrue(pairs.size()==3);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica2 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica4 );

        Assert.assertTrue(pairs.get(1).getCopyFrom() == replica1 );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica4 );

        Assert.assertTrue(pairs.get(2).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(2).getCopyTo() == replica4 );
      }
      else if(copyToName.equals("replica3")){
        Assert.assertTrue(pairs.size()==4);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica4 );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica3 );

        Assert.assertTrue(pairs.get(1).getCopyFrom() == replica2 );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica3 );

        Assert.assertTrue(pairs.get(2).getCopyFrom() == replica1 );
        Assert.assertTrue(pairs.get(2).getCopyTo() == replica3 );

        Assert.assertTrue(pairs.get(3).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(3).getCopyTo() == replica3 );
      }
      else{
        throw new Exception("CopyTo name is invalid " +copyToName);
      }
    }
  }
}
