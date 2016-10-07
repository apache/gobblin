package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigBasedMultiDatasets {

  private final Properties props;
  private final List<ConfigBasedDataset> datasets = new ArrayList<>();
  
  public ConfigBasedMultiDatasets (Config c, Properties props){
    this.props = props;
    
    try {
      FileSystem executionCluster = FileSystem.get(new Configuration());
      URI executionClusterURI = executionCluster.getUri();
      
      ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
      
      //TODO, currently all the staging directory will be created in the target file system ( copyTo )
      // which imply that we need to at least have a distcp-ng job per copyTo cluster
      if(rc.getCopyMode()== ReplicationCopyMode.PUSH){
        log.warn(String.format("Currently not support for push mode of dataset with metadata %s", rc.getMetaData()));
        return;
      }
      
      // PULL mode
      CopyRouteGenerator cpGen = rc.getCopyRouteGenerator();
      List<EndPoint> replicas = rc.getReplicas();
      for(EndPoint replica: replicas){
        if(needGenerateCopyEntity(replica, executionClusterURI)){
          Optional<CopyRoute> copyRoute = cpGen.getPullRoute(rc, replica);
          if(copyRoute.isPresent()){
            this.datasets.add(new ConfigBasedDataset(rc, this.props, copyRoute.get()));
          }
        }
      }

      
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      log.error("Can not create Replication Configuration from raw config "
          + c.root().render(ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)), e);
    } catch (IOException ioe) {
      log.error("Can not decide current execution cluster ", ioe);
      
    }
  }
  
  public List<ConfigBasedDataset> getConfigBasedDatasetList(){
    return this.datasets;
  }
  
  private boolean needGenerateCopyEntity(EndPoint e, URI executionClusterURI){
    if(!(e instanceof HadoopFsEndPoint)){
      return false;
    }
    
    HadoopFsEndPoint ep = (HadoopFsEndPoint)e;
    return ep.getFsURI().equals(executionClusterURI);
  }
}
