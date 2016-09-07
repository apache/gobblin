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
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/missMetaData.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithoutSource(){
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/missSource.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithWrongTopology(){
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/wrongTopology.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testConfigWithWrongTopology2(){
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/wrongTopology2.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==1);
  }
  
  @Test
  public void testValidConfigs(){
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/validCompleteDataset.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    ReplicationMetaData md = rc.getMetaData();
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_JIRA).equals("jira-4455"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_NAME).equals("profileTest"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_OWNER).equals("mitu"));
    
    ReplicationSource source = rc.getSource();
    Assert.assertTrue(source.getReplicationName().equals(ReplicationUtils.REPLICATION_SOURCE));
    Assert.assertTrue(source.isSource());
    Assert.assertTrue(source.getReplicationLocation().getType() == ReplicationType.HDFS);
    Assert.assertTrue(source.getReplicationLocation() instanceof HdfsReplicationLocation);
    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)(source.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("war"));
    Assert.assertTrue(hdfsLocation.getFs_uri().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==4);
    ReplicationReplica replica_holdem = replicas.get(0);
    Assert.assertTrue(replica_holdem.getReplicationName().equals("holdem"));
    hdfsLocation = (HdfsReplicationLocation)(replica_holdem.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("ltx1_holdem"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onHoldem"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
    
    ReplicationReplica replica_uno = replicas.get(1);
    Assert.assertTrue(replica_uno.getReplicationName().equals("uno"));
    hdfsLocation = (HdfsReplicationLocation)(replica_uno.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("ltx1_uno"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onUno"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));

    ReplicationReplica replica_war = replicas.get(2);
    Assert.assertTrue(replica_war.getReplicationName().equals("war"));
    hdfsLocation = (HdfsReplicationLocation)(replica_war.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("lva1_war"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onWar"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    ReplicationReplica replica_tarock = replicas.get(3);
    Assert.assertTrue(replica_tarock.getReplicationName().equals("tarock"));
    hdfsLocation = (HdfsReplicationLocation)(replica_tarock.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("lva1_tarock"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onTarock"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    DataFlowTopology topology = rc.getTopology();
    Assert.assertTrue(topology.getRoutes().size()==4);
    Map<ReplicationReplica, DataFlowTopology.CopyRoute> replicaRoutes = new HashMap<>();
    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
      replicaRoutes.put(route.getCopyDestination(), route);
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
    URL u = getClass().getClassLoader().getResource("replicationConfigTest/validCompleteDatasetWithRoutePicker.conf");
    File configFile = new File(u.getFile());
    Config c = ConfigFactory.parseFile(configFile).resolve();
    
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    ReplicationMetaData md = rc.getMetaData();
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_JIRA).equals("jira-4455"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_NAME).equals("profileTest"));
    Assert.assertTrue(md.getValues().get().get(ReplicationUtils.METADATA_OWNER).equals("mitu"));
    
    ReplicationSource source = rc.getSource();
    Assert.assertTrue(source.getReplicationName().equals(ReplicationUtils.REPLICATION_SOURCE));
    Assert.assertTrue(source.isSource());
    Assert.assertTrue(source.getReplicationLocation().getType() == ReplicationType.HDFS);
    Assert.assertTrue(source.getReplicationLocation() instanceof HdfsReplicationLocation);
    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)(source.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("war"));
    Assert.assertTrue(hdfsLocation.getFs_uri().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    List<ReplicationReplica> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==4);
    ReplicationReplica replica_holdem = replicas.get(0);
    Assert.assertTrue(replica_holdem.getReplicationName().equals("holdem"));
    hdfsLocation = (HdfsReplicationLocation)(replica_holdem.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("ltx1_holdem"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onHoldem"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
    
    ReplicationReplica replica_uno = replicas.get(1);
    Assert.assertTrue(replica_uno.getReplicationName().equals("uno"));
    hdfsLocation = (HdfsReplicationLocation)(replica_uno.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("ltx1_uno"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onUno"));
    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));

    ReplicationReplica replica_war = replicas.get(2);
    Assert.assertTrue(replica_war.getReplicationName().equals("war"));
    hdfsLocation = (HdfsReplicationLocation)(replica_war.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("lva1_war"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onWar"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    ReplicationReplica replica_tarock = replicas.get(3);
    Assert.assertTrue(replica_tarock.getReplicationName().equals("tarock"));
    hdfsLocation = (HdfsReplicationLocation)(replica_tarock.getReplicationLocation());
    Assert.assertTrue(hdfsLocation.getClustername().equals("lva1_tarock"));
    Assert.assertTrue(hdfsLocation.getPath().equals("/data/derived/onTarock"));
    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
    
    DataFlowTopology topology = rc.getTopology();
    Assert.assertTrue(topology.getRoutes().size()==4);
    Map<ReplicationReplica, DataFlowTopology.CopyRoute> replicaRoutes = new HashMap<>();
    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
      replicaRoutes.put(route.getCopyDestination(), route);
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
