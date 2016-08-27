package gobblin.data.management.copy.replication;

import com.google.common.base.Optional;

public abstract class AbstractReplicationData implements ReplicationData{

  protected final String replicationName;
  protected final ReplicationLocation location;
  
  protected AbstractReplicationData(String replicationName, ReplicationLocation location){
    this.replicationName = replicationName;
    this.location = location;
  }
  
  @Override
  public String getReplicationName(){
    return this.replicationName;
  }
  
  @Override
  public ReplicationLocation getReplicationLocation(){
    return this.location;
  }
  
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("isSource:" + this.isSource() + ", ");
    sb.append("name:" + this.getReplicationName() + ", ");
    sb.append("location:[" + this.getReplicationLocation() + "]");
    
    return sb.toString();
  }

}
