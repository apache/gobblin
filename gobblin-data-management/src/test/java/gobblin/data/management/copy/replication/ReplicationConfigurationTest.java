package gobblin.data.management.copy.replication;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Test(groups = {"gobblin.data.management.copy.replication"})
public class ReplicationConfigurationTest {
  @Test
  public void testValidConfigsWithoutMetaData(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/missMetaData.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithoutSource(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/missSource.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithWrongTopology(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/wrongTopology.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithWrongTopology2(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/wrongTopology2.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test
  public void testValidConfigs(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    ReplicationMetaData md = rc.getMetaData();
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_JIRA).equals("jira-4455"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_NAME).equals("profileTest"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_OWNER).equals("mitu"));
    
    SourceEndPoint source = rc.getSource();
    Assert.assertTrue(source.getReplicationName().equals(ReplicationUtils.REPLICATION_SOURCE));
    Assert.assertTrue(source.isSource());
    Assert.assertTrue(source.getReplicationLocation().getType() == ReplicationType.HDFS);
    Assert.assertTrue(source.getReplicationLocation() instanceof HdfsReplicationLocation);
    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)(source.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("war"));
    Assert.assertTrue(hdfsLocation.getFsURI().toString().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==4);
    ReplicaEndPoint replica_holdem = replicas.get(0);
    Assert.assertTrue(replica_holdem.getReplicationName().equals("holdem"));
    hdfsLocation = (HdfsReplicationLocation)(replica_holdem.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_holdem"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onHoldem"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
    
    ReplicaEndPoint replica_uno = replicas.get(1);
    Assert.assertTrue(replica_uno.getReplicationName().equals("uno"));
    hdfsLocation = (HdfsReplicationLocation)(replica_uno.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_uno"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onUno"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));

    ReplicaEndPoint replica_war = replicas.get(2);
    Assert.assertTrue(replica_war.getReplicationName().equals("war"));
    hdfsLocation = (HdfsReplicationLocation)(replica_war.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_war"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onWar"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    ReplicaEndPoint replica_tarock = replicas.get(3);
    Assert.assertTrue(replica_tarock.getReplicationName().equals("tarock"));
    hdfsLocation = (HdfsReplicationLocation)(replica_tarock.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_tarock"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onTarock"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    DataFlowTopology topology = rc.getTopology();
    Assert.assertTrue(topology.getRoutes().size()==4);
    Map<ReplicaEndPoint, DataFlowTopology.CopyRoute> replicaRoutes = new HashMap<>();
    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
      replicaRoutes.put(route.getCopyTo(), route);
    }
    
    // holdem:tarock,war,source
    DataFlowTopology.CopyRoute singleRoute = replicaRoutes.get(replica_holdem);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 3);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==source);
    
    //uno:holdem,tarock,war,source
    singleRoute = replicaRoutes.get(replica_uno);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 4);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_holdem);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(3)==source);
    
    //war:source,
    singleRoute = replicaRoutes.get(replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 1);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==source);
    
    //tarock:war,source,
    singleRoute = replicaRoutes.get(replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 2);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==source);
  }
  
  @Test
  public void testValidConfigsWithRoutePicker(){
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDatasetWithRoutePicker.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    ReplicationMetaData md = rc.getMetaData();
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_JIRA).equals("jira-4455"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_NAME).equals("profileTest"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_OWNER).equals("mitu"));
    
    SourceEndPoint source = rc.getSource();
    Assert.assertTrue(source.getReplicationName().equals(ReplicationUtils.REPLICATION_SOURCE));
    Assert.assertTrue(source.isSource());
    Assert.assertTrue(source.getReplicationLocation().getType() == ReplicationType.HDFS);
    Assert.assertTrue(source.getReplicationLocation() instanceof HdfsReplicationLocation);
    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)(source.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("war"));
    Assert.assertTrue(hdfsLocation.getFsURI().toString().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==4);
    ReplicaEndPoint replica_holdem = replicas.get(0);
    Assert.assertTrue(replica_holdem.getReplicationName().equals("holdem"));
    hdfsLocation = (HdfsReplicationLocation)(replica_holdem.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_holdem"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onHoldem"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
    
    ReplicaEndPoint replica_uno = replicas.get(1);
    Assert.assertTrue(replica_uno.getReplicationName().equals("uno"));
    hdfsLocation = (HdfsReplicationLocation)(replica_uno.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_uno"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onUno"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));

    ReplicaEndPoint replica_war = replicas.get(2);
    Assert.assertTrue(replica_war.getReplicationName().equals("war"));
    hdfsLocation = (HdfsReplicationLocation)(replica_war.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_war"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onWar"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    ReplicaEndPoint replica_tarock = replicas.get(3);
    Assert.assertTrue(replica_tarock.getReplicationName().equals("tarock"));
    hdfsLocation = (HdfsReplicationLocation)(replica_tarock.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_tarock"));
    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onTarock"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    DataFlowTopology topology = rc.getTopology();
    Assert.assertTrue(topology.getRoutes().size()==4);
    Map<ReplicaEndPoint, DataFlowTopology.CopyRoute> replicaRoutes = new HashMap<>();
    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
      replicaRoutes.put(route.getCopyTo(), route);
    }
    
    // holdem:tarock,war,source
    DataFlowTopology.CopyRoute singleRoute = replicaRoutes.get(replica_holdem);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 3);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==source);
    
    //uno:holdem,tarock,war,source
    singleRoute = replicaRoutes.get(replica_uno);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 4);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_holdem);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(3)==source);
    
    //war:source,
    singleRoute = replicaRoutes.get(replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 1);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==source);
    
    //tarock:war,source,
    singleRoute = replicaRoutes.get(replica_tarock);
    Assert.assertTrue(singleRoute.getCopyFrom().size() == 2);
    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_war);
    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==source);
  }
}
