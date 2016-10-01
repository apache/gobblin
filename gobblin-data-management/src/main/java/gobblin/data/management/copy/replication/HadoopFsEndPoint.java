package gobblin.data.management.copy.replication;

public interface HadoopFsEndPoint extends EndPoint{

  /**
   * 
   * @return the hadoop cluster name for {@link EndPoint}s on Hadoop File System 
   */
  public String getClusterName();
}
