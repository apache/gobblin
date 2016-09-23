package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;

/**
 * Extends {@link CopableDataset} to represent data replication dataset based on {@link Config}
 * 
 * Detail logics
 * <ul>
 *  <li>Picked the preferred topology
 *  <li>Based on current running cluster and CopyMode (push or pull) pick the routes
 *  <li>Based on optimization policy to pick the CopyFrom and CopyTo pair
 *  <li>Generated the CopyEntity based on CopyFrom and CopyTo pair
 * </ul>
 * @author mitu
 *
 */
public class ConfigBasedDataset implements CopyableDataset {

  public ConfigBasedDataset(Config c) {

  }

  @Override
  public String datasetURN() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
