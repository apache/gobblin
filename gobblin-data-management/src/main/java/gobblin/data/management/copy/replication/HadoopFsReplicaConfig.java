package gobblin.data.management.copy.replication;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.Getter;


/**
 * Used to encapsulate all the configuration for a hadoop file system replica
 * @author mitu
 *
 */
public class HadoopFsReplicaConfig {
  public static final String HDFS_COLO_KEY = "cluster.colo";
  public static final String HDFS_CLUSTERNAME_KEY = "cluster.name";
  public static final String HDFS_FILESYSTEM_URI_KEY = "cluster.FsURI";
  public static final String HDFS_PATH_KEY = "path";

  @Getter
  private final String colo;

  @Getter
  private final String clustername;

  @Getter
  private final URI fsURI;

  @Getter
  private final Path path;

  public HadoopFsReplicaConfig(Config config) {
    Preconditions.checkArgument(config.hasPath(HDFS_COLO_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_CLUSTERNAME_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_PATH_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_FILESYSTEM_URI_KEY));

    this.colo = config.getString(HDFS_COLO_KEY);
    this.clustername = config.getString(HDFS_CLUSTERNAME_KEY);
    this.path = new Path(config.getString(HDFS_PATH_KEY));
    try {
      this.fsURI = new URI(config.getString(HDFS_FILESYSTEM_URI_KEY));
    } catch (URISyntaxException e) {
      throw new RuntimeException("can not build URI based on " + config.getString(HDFS_FILESYSTEM_URI_KEY));
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("colo", this.colo).add("name", this.clustername)
        .add("FilesystemURI", this.fsURI).add("rootPath", this.path).toString();
  }

}
