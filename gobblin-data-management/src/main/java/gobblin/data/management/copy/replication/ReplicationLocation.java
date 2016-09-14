package gobblin.data.management.copy.replication;

import java.net.URI;

import org.apache.hadoop.fs.Path;


/**
 * Interface ReplicationLocation is used to specify the location of a replication data
 * 
 * @author mitu
 *
 */
public interface ReplicationLocation {

  public ReplicationLocationType getType();

  public String getReplicationLocationName();

  public URI getFsURI();

  /**
   * @return the root path for the replication
   */
  public Path getRootPath();
}
