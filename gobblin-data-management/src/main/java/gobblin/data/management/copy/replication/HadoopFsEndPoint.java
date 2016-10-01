package gobblin.data.management.copy.replication;

import java.net.URI;

public interface HadoopFsEndPoint extends EndPoint{

  /**
   * 
   * @return the hadoop cluster name for {@link EndPoint}s on Hadoop File System 
   */
  public String getClusterName();
  
  public URI getFsURI();
}
