package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class ReplicationConfiguration is used to describe the overall configuration of the replication flow for
 * a specific {@link ReplicationData}, including:
 * <ul>
 *  <li>Meta data 
 *  <li>Replication source
 *  <li>Replication replicas
 *  <li>Replication topology
 * </ul>
 * @author mitu
 *
 */
@AllArgsConstructor
public class ReplicationConfiguration {

  @Getter
  private final ReplicationMetaData metaData;
  
  @Getter
  private final ReplicationSource source;
  
  @Getter
  private final List<ReplicationReplica> replicas;
  
  @Getter
  private final DataFlowTopology topology;
  
  public static ReplicationConfiguration buildFromConfig(Config config){
    if(config == null) return null;
    
//    return new ReplicationConfiguration(buildMetaData(config),
//        buildSource(config), buildReplicas(config), null);
    
    return new Builder()
        .withReplicationMetaData(ReplicationUtils.buildMetaData(config))
        .withReplicationSource(ReplicationUtils.buildSource(config))
        .withReplicationReplicas(ReplicationUtils.buildReplicas(config))
        .withDataFlowTopology(null)
        .build();
  }
  
  private ReplicationConfiguration (Builder builder){
    this.metaData = builder.metaData;
    this.source = builder.source;
    this.replicas = builder.replicas;
    this.topology = builder.topology;
  }
  
  
  
  private static class Builder {
    private ReplicationMetaData metaData;

    private ReplicationSource source;

    private List<ReplicationReplica> replicas;

    private DataFlowTopology topology;

    public Builder withReplicationMetaData(ReplicationMetaData metaData) {
      this.metaData = metaData;
      return this;
    }

    public Builder withReplicationSource(ReplicationSource source) {
      this.source = source;
      return this;
    }
    
    public Builder withReplicationReplicas(List<ReplicationReplica> replicas) {
      this.replicas = replicas;
      return this;
    }
    
    public Builder withDataFlowTopology(DataFlowTopology topology) {
      this.topology = topology;
      return this;
    }

    public ReplicationConfiguration build() {
      return new ReplicationConfiguration(this);
    }
  }
}
