package gobblin.data.management.copy.replication;

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
  public void testValidConfigsInPullMode() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset.conf");
    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
    this.checkReplicationConfig_Pull(rc);
  }
  
  @Test
  public void testValidConfigsInPushMode() throws Exception{
    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDataset_PushMode.conf");
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
    Assert.assertTrue(innerConf.getClustername().equals("war"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/jobs/mitu/profileTest"));
    Assert.assertTrue(innerConf.getColo().equals("lva1"));
    
    List<EndPoint> replicas = rc.getReplicas(); 
    Assert.assertTrue(replicas.size()==4);
    EndPoint replica_holdem = replicas.get(0);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_holdem).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("holdem"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://ltx1-holdemnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/onHoldem"));
    Assert.assertTrue(innerConf.getColo().equals("ltx1"));
    
    EndPoint replica_uno = replicas.get(1);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_uno).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("uno"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://ltx1-unonn01.grid.linkedin.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/onUno"));
    Assert.assertTrue(innerConf.getColo().equals("ltx1"));
    
    EndPoint replica_war = replicas.get(2);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_war).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("war"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/onWar"));
    Assert.assertTrue(innerConf.getColo().equals("lva1"));
    
    EndPoint replica_tarock = replicas.get(3);
    innerConf = ((ReplicaHadoopFsEndPoint)replica_tarock).getRc();
    Assert.assertTrue(innerConf.getClustername().equals("tarock"));
    Assert.assertTrue(innerConf.getFsURI().toString().equals("hdfs://ltx1-tarocknn01.grid.linkedin.com:9000"));
    Assert.assertTrue(innerConf.getPath().toString().equals("/data/derived/onTarock"));
    Assert.assertTrue(innerConf.getColo().equals("lva1"));
  }
  
  private void checkReplicationConfig_Push(ReplicationConfiguration rc) throws Exception{
    ReplicationCopyMode copyMode = rc.getCopyMode();
    Assert.assertTrue(copyMode == ReplicationCopyMode.PUSH);
   
    checkReplicationConfigStaticPart(rc);
    
    List<EndPoint> replicas = rc.getReplicas();
    EndPoint source = rc.getSource();
    EndPoint replica_holdem = replicas.get(0);
    EndPoint replica_uno = replicas.get(1);
    EndPoint replica_war = replicas.get(2);
    EndPoint replica_tarock = replicas.get(3);
    
    DataFlowTopology topology = rc.getDataFlowToplogy();
    Assert.assertTrue(topology.getDataFlowPaths().size()==4);
    
    for(DataFlowTopology.DataFlowPath p: topology.getDataFlowPaths()){
      List<EndPointCopyPair> pairs = p.getCopyPairs();
      Assert.assertTrue(!pairs.isEmpty());
      String copyFromName = pairs.get(0).getCopyFrom().getEndPointName();
      
      if(copyFromName.equals("war")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_war );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_tarock );
      }
      else if(copyFromName.equals("tarock")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_tarock );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_holdem );
      }
      else if(copyFromName.equals("holdem")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_holdem );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_uno );
      }
      else if(copyFromName.equals(ReplicationConfiguration.REPLICATION_SOURCE)){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_war );
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
    EndPoint replica_holdem = replicas.get(0);
    EndPoint replica_uno = replicas.get(1);
    EndPoint replica_war = replicas.get(2);
    EndPoint replica_tarock = replicas.get(3);
    
    DataFlowTopology topology = rc.getDataFlowToplogy();
    Assert.assertTrue(topology.getDataFlowPaths().size()==4);
    
    for(DataFlowTopology.DataFlowPath p: topology.getDataFlowPaths()){
      List<EndPointCopyPair> pairs = p.getCopyPairs();
      Assert.assertTrue(!pairs.isEmpty());
      String copyToName = pairs.get(0).getCopyTo().getEndPointName();
      
      if(copyToName.equals("war")){
        Assert.assertTrue(pairs.size()==1);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_war );
      }
      else if(copyToName.equals("tarock")){
        Assert.assertTrue(pairs.size()==2);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_war );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_tarock );
        
        Assert.assertTrue(pairs.get(1).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica_tarock );
      }
      else if(copyToName.equals("holdem")){
        Assert.assertTrue(pairs.size()==3);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_tarock );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_holdem );
        
        Assert.assertTrue(pairs.get(1).getCopyFrom() == replica_war );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica_holdem );
        
        Assert.assertTrue(pairs.get(2).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(2).getCopyTo() == replica_holdem );
      }
      else if(copyToName.equals("uno")){
        Assert.assertTrue(pairs.size()==4);
        Assert.assertTrue(pairs.get(0).getCopyFrom() == replica_holdem );
        Assert.assertTrue(pairs.get(0).getCopyTo() == replica_uno );
        
        Assert.assertTrue(pairs.get(1).getCopyFrom() == replica_tarock );
        Assert.assertTrue(pairs.get(1).getCopyTo() == replica_uno );
        
        Assert.assertTrue(pairs.get(2).getCopyFrom() == replica_war );
        Assert.assertTrue(pairs.get(2).getCopyTo() == replica_uno );
        
        Assert.assertTrue(pairs.get(3).getCopyFrom() == source );
        Assert.assertTrue(pairs.get(3).getCopyTo() == replica_uno );
      }
      else{
        throw new Exception("CopyTo name is invalid " +copyToName);
      }
    }
  }
  
//  @Test
//  public void testValidConfigsWithRoutePicker(){
//    Config c = ConfigFactory.parseResources(getClass().getClassLoader(), "replicationConfigTest/validCompleteDatasetWithRoutePicker.conf");
//    ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
//    ReplicationMetaData md = rc.getMetaData();
//    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_JIRA).equals("jira-4455"));
//    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_NAME).equals("profileTest"));
//    Assert.assertTrue(md.getValues().get().get(ReplicationConfiguration.METADATA_OWNER).equals("mitu"));
//    
//    SourceEndPoint source = rc.getSource();
//    Assert.assertTrue(source.getReplicationName().equals(ReplicationConfiguration.REPLICATION_SOURCE));
//    Assert.assertTrue(source.isSource());
//    Assert.assertTrue(source.getReplicationLocation().getType() == ReplicationLocationType.HADOOPFS);
//    Assert.assertTrue(source.getReplicationLocation() instanceof HdfsReplicationLocation);
//    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)(source.getReplicationLocation());
//    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("war"));
//    Assert.assertTrue(hdfsLocation.getFsURI().toString().equals("hdfs://ltx1-warnn01.grid.linkedin.com:9000"));
//    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/jobs/mitu/profileTest"));
//    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
//    
//    List<ReplicaEndPoint> replicas = rc.getReplicas(); 
//    Assert.assertTrue(replicas.size()==4);
//    ReplicaEndPoint replica_holdem = replicas.get(0);
//    Assert.assertTrue(replica_holdem.getReplicationName().equals("holdem"));
//    hdfsLocation = (HdfsReplicationLocation)(replica_holdem.getReplicationLocation());
//    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_holdem"));
//    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onHoldem"));
//    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
//    
//    ReplicaEndPoint replica_uno = replicas.get(1);
//    Assert.assertTrue(replica_uno.getReplicationName().equals("uno"));
//    hdfsLocation = (HdfsReplicationLocation)(replica_uno.getReplicationLocation());
//    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("ltx1_uno"));
//    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onUno"));
//    Assert.assertTrue(hdfsLocation.getColo().equals("ltx1"));
//
//    ReplicaEndPoint replica_war = replicas.get(2);
//    Assert.assertTrue(replica_war.getReplicationName().equals("war"));
//    hdfsLocation = (HdfsReplicationLocation)(replica_war.getReplicationLocation());
//    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_war"));
//    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onWar"));
//    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
//    
//    ReplicaEndPoint replica_tarock = replicas.get(3);
//    Assert.assertTrue(replica_tarock.getReplicationName().equals("tarock"));
//    hdfsLocation = (HdfsReplicationLocation)(replica_tarock.getReplicationLocation());
//    Assert.assertTrue(hdfsLocation.getReplicationLocationName().equals("lva1_tarock"));
//    Assert.assertTrue(hdfsLocation.getRootPath().toString().equals("/data/derived/onTarock"));
//    Assert.assertTrue(hdfsLocation.getColo().equals("lva1"));
//    
//    DataFlowTopology topology = rc.getTopology();
//    Assert.assertTrue(topology.getRoutes().size()==4);
//    Map<ReplicaEndPoint, DataFlowTopology.CopyRoute> replicaRoutes = new HashMap<>();
//    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
//      replicaRoutes.put(route.getCopyTo(), route);
//    }
//    
//    // holdem:tarock,war,source
//    DataFlowTopology.CopyRoute singleRoute = replicaRoutes.get(replica_holdem);
//    Assert.assertTrue(singleRoute.getCopyFrom().size() == 3);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_tarock);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_war);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==source);
//    
//    //uno:holdem,tarock,war,source
//    singleRoute = replicaRoutes.get(replica_uno);
//    Assert.assertTrue(singleRoute.getCopyFrom().size() == 4);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_holdem);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==replica_tarock);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(2)==replica_war);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(3)==source);
//    
//    //war:source,
//    singleRoute = replicaRoutes.get(replica_war);
//    Assert.assertTrue(singleRoute.getCopyFrom().size() == 1);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==source);
//    
//    //tarock:war,source,
//    singleRoute = replicaRoutes.get(replica_tarock);
//    Assert.assertTrue(singleRoute.getCopyFrom().size() == 2);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(0)==replica_war);
//    Assert.assertTrue(singleRoute.getCopyFrom().get(1)==source);
//
//    Assert.assertTrue(rc.getCopyMode() == ReplicationCopyMode.PULL);
//  }
}
